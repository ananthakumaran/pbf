(ns pbf.worker
  (:require [taoensso.timbre :as log]))
(import java.util.concurrent.Semaphore)

(defn create-woker
  ([]
     (create-woker (.availableProcessors (Runtime/getRuntime))))
  ([limit]
     (let [semaphore (Semaphore. limit)]
       (fn [work]
         (.acquire semaphore)
         (try
           (future (try (work)
                        (catch Throwable e
                          (log/error (.printStackTrace e) "worker failed")
                          (throw e))
                        (finally (.release semaphore))))
           (catch Throwable e
             (.release semaphore)
             (throw e)))))))
