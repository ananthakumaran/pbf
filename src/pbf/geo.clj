(ns pbf.geo)

(defn- to-rad [v]
  (* v (/ Math/PI 180)))

;; http://www.movable-type.co.uk/scripts/latlong.html
(defn distance [lat1 lon1 lat2 lon2]
  (let [dlat (to-rad (- lat2 lat1))
        dlon (to-rad (- lon2 lon1))
        sindlat (Math/sin (/ dlat 2))
        sindlon (Math/sin (/ dlon 2))
        a (+ (* sindlat sindlat)
             (* sindlon sindlon (Math/cos (to-rad lat1)) (Math/cos (to-rad lat2))))
        c (* 2 (Math/atan2 (Math/sqrt a) (Math/sqrt (- 1 a))))]
    (* 6371 c)))


