(ns pbf.core
  (:gen-class)
  (:use [pbf worker geo])
  (:use flatland.protobuf.core)
  (:use clojure.java.io)
  (:require [taoensso.timbre :as log]))

(import java.util.zip.Inflater)
(import java.util.Date)

(import crosby.binary.Fileformat$BlobHeader)
(import crosby.binary.Fileformat$Blob)
(import crosby.binary.Osmformat$HeaderBlock)
(import crosby.binary.Osmformat$PrimitiveBlock)

(def BlobHeader (protodef Fileformat$BlobHeader))
(def Blob (protodef Fileformat$Blob))
(def HeaderBlock (protodef Osmformat$HeaderBlock))
(def PrimitiveBlock (protodef Osmformat$PrimitiveBlock))

;;; utils

(defn byte-array-to-int
  "convert 4 byte array into int. follows big-endian format"
  [bytes]
  (reduce (fn [r i]
            (let [n (bit-shift-left (bit-and (nth bytes (- 3 i)) 0xFF) (* i 8))]
              (bit-or r n)))
          0 (range 0 4)))

(defn fill-bytes
  "tries to fill the bytes array with data from stream.
   return false if there is shortage of data"
  [stream bytes]
  (let [to-read (count bytes)
        read (.read stream bytes 0 to-read)]
    (= to-read read)))

(defn zlib-decompress
  [input original-size]
  (let [output (byte-array original-size)
        decompressor (Inflater.)]
    (do
      (. decompressor setInput input)
      (. decompressor inflate output)
      output)))

(defn next-block [stream]
  (let [header-size (byte-array 4)]
    (when (fill-bytes stream header-size)
      (let [header-bytes (byte-array (byte-array-to-int header-size))]
        (when (fill-bytes stream header-bytes)
          (let [blob-header (protobuf-load BlobHeader header-bytes)
                blob-bytes (byte-array (:datasize blob-header))]
            (when (fill-bytes stream blob-bytes)
              [blob-header (protobuf-load Blob blob-bytes)])))))))


;; handle raw data
(defn blob-to-block [blob type]
  (protobuf-load type (zlib-decompress (. (:zlib-data blob) toByteArray)
                                       (:raw-size blob))))

(defn delta-decode [items]
  (second (reduce (fn [[l r] c]
                    (let [n (+ l c)]
                      [n (conj r n)]))
                  [0 []]
                  items)))

;; handle empty keys_vals
(defn keys-vals-map
  ([keys-vals string-table]
     (keys-vals-map keys-vals string-table []))
  ([keys-vals string-table result]
     (if (empty? keys-vals)
       result
       (let [[current rest] (split-with #(not (= 0 %)) keys-vals)
             hash (apply hash-map (map (partial nth string-table) current))]
         (recur (drop 1 rest) string-table (conj result hash))))))

(defn p-keys-vals-map [tags-table keys vals]
  (let [lookup (partial nth tags-table)]
    (into {} (map #(-> [(lookup %1) (lookup %2)]) keys vals))))

(defn string-table [primitive-block]
  (map #(. % toStringUtf8) (:s (:stringtable primitive-block))))

(defn degree-calculator [offset granularity]
  (fn [val]
    (* 0.000000001 (+ offset (* granularity val)))))

(defn timestamp-calculator [granularity]
  (if granularity
    (fn [t]
      (when t
        (Date. (* t granularity))))
    (fn [t] nil)))

(defrecord Group [nodes ways relations])
(defrecord Node [id lat lon tags])
(defrecord Way [id refs tags info])
(defrecord Member [id role type])
(defrecord Relation [id tags members info])
(defrecord Info [id version timestamp changeset user visible])

(defn info [i tags-table timestamp-calculator]
  (when i
    (Info. (:uid i)
           (:version i)
           (timestamp-calculator (:timestamp i))
           (:changeset i)
           (nth tags-table (:user_sid i))
           (:visible i))))

(defn dense-map [dense tags-table latc lonc]
  (map (fn [id lat lon keys_vals]
         (Node. id (latc lat) (lonc lon) keys_vals))
       (delta-decode (:id dense))
       (delta-decode (:lat dense))
       (delta-decode (:lon dense))
       (keys-vals-map (:keys_vals dense) tags-table)))

(defn nodes [primitive-group tags-table lat-calculator lon-calculator]
  (concat []
          (dense-map (:dense primitive-group) tags-table lat-calculator lon-calculator)))

(defn ways [primitive-group tags-table t-calculator]
  (map (fn [way]
         (Way. (:id way)
               (delta-decode (:refs way))
               (p-keys-vals-map tags-table (:keys way) (:vals way))
               (info (:info way) tags-table t-calculator)))
       (:ways primitive-group)))

(defn relations [primitive-group tags-table t-calculator]
  (map (fn [relation]
         (Relation. (:id relation)
                    (p-keys-vals-map tags-table (:keys relation) (:vals relation))
                    (map (fn [role-id member-id type]
                           (Member. member-id (nth tags-table role-id) type))
                         (:roles_sid relation)
                         (delta-decode (:memids relation))
                         (:types relation))
                    (info (:info relation) tags-table t-calculator)))
       (:relations primitive-group)))

(defn walk [stream types callback]
  (when-let [block (next-block stream)]
    (let [[header blob] block]
      (case (:type header)
        "OSMHeader" nil
        "OSMData"
        (let [primitive-block (blob-to-block blob PrimitiveBlock)
              lat-calculator (degree-calculator (:lat_offset primitive-block) (:granularity primitive-block))
              lon-calculator (degree-calculator (:lon_offset primitive-block) (:granularity primitive-block))
              t-calculator (timestamp-calculator (:date_granularity primitive-block))
              primitive-groups (:primitivegroup primitive-block)
              tags-table (string-table primitive-block)]
          (doseq [primitive-group primitive-groups]
            (callback (Group. (if (some #{:node} types)
                                (nodes primitive-group tags-table lat-calculator lon-calculator)
                                '())

                              (if (some #{:way} types)
                                (ways primitive-group tags-table t-calculator)
                                '())

                              (if (some #{:relation} types)
                                (relations primitive-group tags-table t-calculator)
                                '()))))))
      (recur stream types callback))))


;; test
(defn -main []
  (let [worker (create-worker)]
    (with-open [stream (input-stream "/Users/ananthakumaran/work/pbf/india.osm.pbf")]
      (walk stream [:way :relation :node]
            (fn [group]
              (worker
               (fn []
                 (when (not (empty? (:relations group)))
                   (log/info "relations callback ---")
                   (log/info (print-str (count (:relations group)))))
                 (when (not (empty? (:ways group)))
                   (log/info "ways callback ---")
                   (log/info (print-str (count (:ways group)))))
                 (when (not (empty? (:nodes group)))
                   (log/info "nodes callback ---")
                   (log/info (print-str (count (:nodes group)))))
                 #_(doseq [node (:nodes group)]
                     (if (and (contains? (:tags node) "amenity")
                              (let [amenity (get (:tags node) "amenity")]
                                (some #(= amenity %) ["bar" "cafe" "fast_food" "ice_cream" "pub" "restaurant"]))
                              (< (distance (:lat node) (:lon node) 12.9833 77.5833) 20))
                       (println node)))))))
      (shutdown-agents))))
