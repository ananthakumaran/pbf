(ns pbf.core
  (:gen-class)
  (:use [flatland.protobuf.core])
  (:use clojure.java.io))

(import java.util.zip.Inflater)

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

(defn string-table [primitive-block]
  (map #(. % toStringUtf8) (:s (:stringtable primitive-block))))

(defn degree-calculator [offset granularity]
  (fn [val]
    (* 0.000000001 (+ offset (* granularity val)))))

(defn dense-map [dense string-table latc lonc]
  (map (fn [id lat lon keys_vals]
         {:id id, :lat (latc lat), :lon (lonc lon), :keys_vals keys_vals})
       (delta-decode (:id dense))
       (delta-decode (:lat dense))
       (delta-decode (:lon dense))
       (keys-vals-map (:keys_vals dense) string-table)))

;; todo handle nodes,ways,relations,changesets in PrimitiveGroup
(defn node-walker [stream callback]
  (when-let [block (next-block stream)]
    (let [[header blob] block]
      (case (:type header)
        "OSMHeader" (println "headerblock")
        "OSMData"
        (let [primitive-block (blob-to-block blob PrimitiveBlock)]
          (callback (dense-map (:dense (first (:primitivegroup primitive-block)))
                               (string-table primitive-block)
                               (degree-calculator (:lat_offset primitive-block) (:granularity primitive-block))
                               (degree-calculator (:lon_offset primitive-block) (:granularity primitive-block))))))
      (recur stream callback))))

(defn to-rad [v]
  (* v (/ Math/PI 180)))

;; http://www.movable-type.co.uk/scripts/latlong.html
(defn distance [lat1 lon1 lat2 lon2]
  (let [dlat (to-rad (- lat2 lat1))
        dlon (to-rad (- lon2 lon1))
        a (+ (* (Math/sin (/ dlat 2))
                (Math/sin (/ dlat 2)))
             (* (Math/sin (/ dlon 2))
                (Math/sin (/ dlon 2))
                (Math/cos (to-rad lat1))
                (Math/cos (to-rad lat2))))
        c (* 2 (Math/atan2 (Math/sqrt a) (Math/sqrt (- 1 a))))]
    (* 6371 c)))



;; test
(defn -main []
  (with-open [stream (input-stream "/Users/ananthakumaran/work/pbf/india.osm.pbf")]
    (node-walker stream (fn [nodes]
                          (doseq [node nodes]
                            (if (and (contains? (:keys_vals node) "amenity")
                                     (= (get (:keys_vals node) "amenity") "restaurant")
                                     (< (distance (:lat node) (:lon node) 12.9833 77.5833) 20))
                              (println node)))))))
