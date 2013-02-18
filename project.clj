(defproject pbf "0.1.0-SNAPSHOT"
  :description "openstreetmap pbf parser"
  :url "https://github.com/ananthakumaran/pbf"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main pbf.core
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.flatland/protobuf "0.7.2"]
                 [com.taoensso/timbre "1.5.2"]]
  :plugins [[lein-protobuf "0.3.1"]])
