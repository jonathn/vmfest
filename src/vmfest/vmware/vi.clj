(ns vmfest.vmware.vi
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [clojure.pprint :as pp]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import [com.vmware.vim25.mo ServiceInstance InventoryNavigator]
           [com.vmware.vim25 TaskInfoState]
           [java.net URL]))

(defn sleep [seconds]
  (java.lang.Thread/sleep (* seconds 1000)))

;;----------------------------------------------------------------------------
(defn get-progress-percent [task-info]
  (if (#{TaskInfoState/success TaskInfoState/error} (.getState task-info))
    100
    (or (.getProgress task-info)
        0)))

(defn wait-for-task [task description]
  (let [task-info (.getTaskInfo task)]
    (condp = (.getState task-info)
      TaskInfoState/error
      (if-let [err (.getError task-info)]
        (throw+ {:type ::task-error
                 :cause (.getFault err)} (.getFault err) "Error in task %s: %s"  ;TODO: improve (eg.for power-on when already on)
                description (.getLocalizedMessage err))
        (throw+ {:type ::task-error} "Error in task %s" description))

      TaskInfoState/success
      (do (log/debugf "%s: Success" description)
          "success")

      TaskInfoState/queued
      (do (log/debugf "%s: Queued" description)
          (sleep 10)
          (recur task description))

      TaskInfoState/running
      (do (log/debugf "%s: Running (%s%%)" description
                      (get-progress-percent task-info))
          (sleep 10)
          (recur task description)))))

(defmacro with-mor-workarounds
  "this macro exists because vCenter sometimes fails to get the
managed object reference for objects that actually do exist. Wrap
any call to vi java in this to have it retry with an incramental delay"
  [& body]
  `((fn mor-workaround# [attempt# timeout#]
      (if (> attempt# 10)
        (do (println "giving up after too many mor-not-found failures")
            (println "please complain to VMware about this bug...")
            (throw (Exception. "MOR not found for existing object")))
        (try
          ~@body
          (catch RuntimeException e#
            (if  (instance? com.vmware.vim25.ManagedObjectNotFound (unwrap-exception e#))
              (do (println "Caught VMware ManagedObjectNotFound bug " e#)
                  (sleep timeout#)
                  (mor-workaround# (inc attempt#) (+ 5 timeout#)))
              (throw (RuntimeException. e#)))))))
    0 5))

(defn connect [host-ip-or-dns username password]
  (let [url (str "https://" host-ip-or-dns "/sdk")]
    (log/debugf "Logging in to vmware at '%s' as '%s'" url username)
    {:si (ServiceInstance. (java.net.URL. url) username password true)
     :url url
     :username username
     :password password}))

;;----------------------------------------------------------------------------
(def ^:dynamic *vc-conn* nil)
(def vc-conn (atom nil))

(defn vc-login [vc username password]
  (reset! vc-conn (connect vc username password)))

(defmacro with-vc-login
  "Logs into a VC, runs the operations in the body and then logs out"
  [creds & body]
  `(let [{vc# :vc username# :username password# :password} ~creds]
     (binding [*vc-conn* (connect vc# username# password#)]
       (try
         ~@body
         (finally
           (-> (:si *vc-conn*) .getServerConnection .logout))))))

(defn get-si
  "if a VC connection exists for this thread use that,
  otherwise use the globally set connection"
  [& [opts]]
  (or (get-in opts [:conn :si]) (:si *vc-conn*) (:si @vc-conn)))

;;----------------------------------------------------------------------------
(defn root-inventory-nav [& [opts]]
  (new InventoryNavigator (.getRootFolder (get-si opts))))

(defn get-vms [& [opts]]
  (let [vc-inv (new InventoryNavigator (.getRootFolder (get-si opts)))]
    (.searchManagedEntities vc-inv "VirtualMachine")))

(defn get-dcs [& [opts]]
  (let [vc-inv (new InventoryNavigator (.getRootFolder (get-si opts)))]
    (.searchManagedEntities vc-inv "Datacenter")))

(defn get-hosts [& [opts]]
  (let [vc-inv (new InventoryNavigator (.getRootFolder (get-si opts)))]
    (.searchManagedEntities vc-inv "HostSystem")))


(defn get-datastores [& [opts]]
  (let [vc-inv (new InventoryNavigator (.getRootFolder (get-si opts)))]
    (seq (.searchManagedEntities vc-inv "Datastore"))))


(defn get-vm-in [start-point vm-name]
  "Find vm within <start-point> container. Returns nil if not found"
  (.searchManagedEntity (InventoryNavigator. start-point) "VirtualMachine" vm-name))

(defn get-vm-by-name [vm-name & [opts]]
  (get-vm-in (.getRootFolder (get-si opts)) vm-name))

(defn get-datastore-by-name [datastore-name & [opts]]
  (.searchManagedEntity (root-inventory-nav opts) "Datastore" datastore-name))

(defn get-host-by-name [name & [opts]]
  (first (filter #(= (.getName %) name) (get-hosts opts))))

;|(defn get-datastores-by-host-and-name [host-name datastore-name]
;|  (filter #(= datastore-name (.getName %))
;|          (.getDatastores (get-host-by-name host-name))))
;|
;|(defn get-network-by-host-and-name [host-name net-name]
;|  (first (filter #(= (.getName %) net-name)
;|                 (.getNetworks (get-host-by-name host-name )))))

(defn vms-in-testbed [search-regex & [opts]]
  (filter #(not-empty (re-find search-regex
                               (.getName %)))
          (get-vms opts)))

(defn get-vm-datastores [vm]
  (map #(.getName (.getSummary %)) (.getDatastores vm)))

;|(defn suspend-vm [vm-name]
;|  (wait-for-task (.suspendVM_Task (get-vm-by-name vm-name)) (str "Suspend " vm-name)))

(defn get-vms-on-datastore [datastore-names-set & [opts]]
  (filter (fn [vm] (some #(datastore-names-set %)
                             (get-vm-datastores vm)))
               (get-vms opts)))

;|(defn snapshot-vm [vm-name snapshot-name]
;|  (.waitForMe
;|   (.createSnapshot_Task (get-vm-by-name vm-name)
;|                         snapshot-name snapshot-name false false)))
;|(defn get-snapshot [vm-name snapshot-name]
;|  (first (filter #(= (.getDescription %) snapshot-name)
;|                 (.rootSnapshotList (.getSnapshot (get-vm-by-name vm-name))))))
;|
;|(defn revert-to-current-snapshot [vm-name host-name]
;|  (.waitForMe (.revertToCurrentSnapshot_Task
;|               (get-vm-by-name vm-name) (get-host-by-name host-name))))

;;(defn delete-snapshot)

(defn delete [vm-to-delete]
  (log/infof "Deleting node: %s" (.getName vm-to-delete))
  (try (wait-for-task (.powerOffVM_Task vm-to-delete)
                      (str "Power off " (.getName vm-to-delete))) ; turn off
       (catch com.vmware.vim25.InvalidPowerState _))   ; already-off?
  (try (wait-for-task (.destroy_Task vm-to-delete)
                      (str "Delete " (.getName vm-to-delete))) ; delete files
       (catch com.vmware.vim25.FileNotFound _)))   ; already-deleted?

(defn power-on
  [vm-to-start host]
  (log/infof "Starting node: %s" (.getName vm-to-start))
  (try+ (wait-for-task (.powerOnVM_Task vm-to-start host)
                       (str "Start " (.getName vm-to-start))) ; turn on
        (catch (instance? com.vmware.vim25.InvalidPowerState (:cause %)) _)  ;TODO: improve
        (catch com.vmware.vim25.InvalidPowerState _))) ; already-on?

;|(defn power-on-by-name [vm-name host-name]
;|   (power-on (get-vm-by-name vm-name) (get-host-by-name host-name)))
;|
;|(defn delete-by-name [name]
;|  (let [vm-to-delete (get-vm-by-name name)]
;|    (if (nil? vm-to-delete)
;|      (println "no vm " name "exists to be deleted")
;|      (delete vm-to-delete))))
;|
;|(defn delete-testbed [pattern]
;|  (println "deleting testbed " pattern)
;|  (doall (pmap delete (vms-in-testbed pattern))))
;|
;|(defn start-testbed [pattern host]
;|  (doall (pmap #(power-on % (get-host-by-name host)) (vms-in-testbed pattern))))

(defn powered-off [vms]
  (doall (filter #(= (-> % .getRuntime .powerState)
                          com.vmware.vim25.VirtualMachinePowerState/poweredOff)
                 vms)))

(defn powered-on [vms]
  (doall (filter #(= (-> % .getRuntime .powerState)
                          com.vmware.vim25.VirtualMachinePowerState/poweredOn)
                 vms)))

(defn names [vms] (map #(.getName %) vms))

(defn get-ip [vm]
  (-> vm .getGuest .getIpAddress))

;;###
;;(defn power [state vm]
;;  true)
;;(defn get-ip [vm]
;;  "1.2.3.4")
;;###

(defn get-ips [vms]
  (map get-ip vms))

(defn get-names-and-ips [& [opts]]
  (-> (get-vms opts)
      powered-on
      (#(zipmap (names %) (get-ips %)))))

;|(defn get-devices [vm-name]
;|  (-> (get-vm-by-name vm-name) .getConfig .getHardware .getDevice))
;|
;|(defn get-device-by-name [vm-name dev-name]
;|  (first (filter #(= (.getLabel (.deviceInfo %)) dev-name) (get-devices vm-name))))
;|
;|(defn make-net-dev [host-name vm-name net-name device-name]
;|  (let [network (get-network-by-host-and-name host-name net-name)
;|        nic-backing-info (com.vmware.vim25.VirtualEthernetCardNetworkBackingInfo.)
;|        dev-connect-info (com.vmware.vim25.VirtualDeviceConnectInfo.)
;|        nic (com.vmware.vim25.VirtualE1000.)
;|        nic-dev-spec (com.vmware.vim25.VirtualDeviceConfigSpec.)
;|        original-dev (get-device-by-name vm-name device-name)]
;|    (if (= (type network) com.vmware.vim25.mo.DistributedVirtualPortgroup)
;|      (.setDeviceName nic-backing-info (.getKey (.getConfig network))) ;workaround for VDSwitches
;|      (.setDeviceName nic-backing-info net-name))
;|    (.setNetwork nic-backing-info (.getMOR network))
;|    (doto dev-connect-info
;|      (.setAllowGuestControl true)
;|      (.setConnected true)
;|      (.setStartConnected true))
;|    (doto nic
;|      (.setBacking nic-backing-info)
;|      (.setKey (.getKey original-dev))
;|      (.setUnitNumber (.getUnitNumber original-dev))
;|      (.setAddressType "generated")
;|      (.setConnectable dev-connect-info))
;|    (doto nic-dev-spec
;|      (.setDevice nic)
;|      (.setOperation com.vmware.vim25.VirtualDeviceConfigSpecOperation/edit))
;|    nic-dev-spec))
;|
;|(defn clone-vm [old-name new-name host-name datastore-name net-names]
;|  (let [vm (get-vm-by-name old-name)
;|        destination-host (get-host-by-name  host-name)
;|        datastore (first (get-datastores-by-host-and-name host-name datastore-name))
;|        pool (-> destination-host .getParent .getResourcePool)
;|        folder (.getParent vm)
;|        relocate-spec (com.vmware.vim25.VirtualMachineRelocateSpec.)
;|        config-spec (com.vmware.vim25.VirtualMachineConfigSpec.)
;|        clone-spec (com.vmware.vim25.VirtualMachineCloneSpec.)]
;|    (doto relocate-spec
;|      (.setDatastore (.getMOR datastore))
;|      (.setPool (.getMOR pool))
;|      ;(.setDiskMoveType "moveChildMostDiskBacking")
;|      (.setDiskMoveType "moveAllDiskBackingsAndAllowSharing")
;|      (.setHost (.getMOR destination-host)))
;|    (doto config-spec
;|      (.setName new-name)
;|      (.setMemoryMB (Long. 2048))
;|      (.setNumCPUs (Integer. 2))
;|      (.setDeviceChange
;|       (into-array com.vmware.vim25.VirtualDeviceConfigSpec
;|                   (map (fn [[dev net]]
;|                          (make-net-dev host-name old-name net dev))
;|                        net-names))))
;|    (doto clone-spec
;|      (.setPowerOn false)
;|      (.setTemplate false)
;|      (.setLocation relocate-spec)
;|      (.setConfig config-spec))
;|    (let [task (.cloneVM_Task vm folder new-name clone-spec)]
;|      (wait-for-task task (str "Clone " new-name)))))

(defn clone-vm [src-vm-name new-vm-name host-name datastore-name net-names & [opts]]
  (let [vm (or (get-vm-by-name src-vm-name opts)
               (throw+ {:type ::vm-not-found} "Source VM not found: %s" src-vm-name))
        ;;;destination-host (get-host-by-name  host-name)
        ;;;datastore (first (get-datastores-by-host-and-name host-name datastore-name))
        datastore (get-datastore-by-name datastore-name opts)
        ;;;pool (-> destination-host .getParent .getResourcePool)
        folder (.getParent vm)
        relocate-spec (com.vmware.vim25.VirtualMachineRelocateSpec.)
        config-spec (com.vmware.vim25.VirtualMachineConfigSpec.)
        clone-spec (com.vmware.vim25.VirtualMachineCloneSpec.)]
    (doto relocate-spec
      (.setDatastore (.getMOR datastore))
      ;;;(.setPool (.getMOR pool))
      ;(.setDiskMoveType "moveChildMostDiskBacking")
      (.setDiskMoveType "moveAllDiskBackingsAndAllowSharing")
      ;;;(.setHost (.getMOR destination-host))
      )
    (doto config-spec
      (.setName new-vm-name)
      (.setMemoryMB (Long. 2048))
      (.setNumCPUs (Integer. 2))
      (.setDeviceChange
       (into-array com.vmware.vim25.VirtualDeviceConfigSpec
                   ;;;(map (fn [[dev net]]
                   ;;;       (make-net-dev host-name src-vm-name net dev))
                   ;;;     net-names)
                   []
                   )))
    (doto clone-spec
      (.setPowerOn false)
      (.setTemplate false)
      (.setLocation relocate-spec)
      (.setConfig config-spec))
    (let [task (.cloneVM_Task vm folder new-vm-name clone-spec)]
      (wait-for-task task (str "Clone " new-vm-name)))))

;|(defn plug-in-network [vm-name device-name]
;|  (let [config-spec (com.vmware.vim25.VirtualMachineConfigSpec.)
;|        nic (get-device-by-name vm-name device-name)
;|        nic-dev-spec (com.vmware.vim25.VirtualDeviceConfigSpec.)]
;|    (.setConnected (.getConnectable nic) false)
;|    (doto nic-dev-spec
;|      (.setDevice nic)
;|      (.setOperation com.vmware.vim25.VirtualDeviceConfigSpecOperation/edit))
;|    (.setDeviceChange config-spec (into-array com.vmware.vim25.VirtualDeviceConfigSpec [nic-dev-spec]))
;|    (.waitForMe (.reconfigVM_Task (get-vm-by-name vm-name) config-spec))))

(def power-states
  {com.vmware.vim25.VirtualMachinePowerState/poweredOn :poweredOn
   :poweredOn com.vmware.vim25.VirtualMachinePowerState/poweredOn
   com.vmware.vim25.VirtualMachinePowerState/poweredOff :poweredOff
   :poweredOff com.vmware.vim25.VirtualMachinePowerState/poweredOff
   com.vmware.vim25.VirtualMachinePowerState/suspended :suspended
   :suspended com.vmware.vim25.VirtualMachinePowerState/suspended})

(defn get-power-state [vm]
  (get power-states (-> vm .getRuntime .powerState)))

;|(defn report
;|  ([] (report (range 1 41)))
;|  ([testbed-numbers]
;|     (zipmap (map #(str "testbed" %) testbed-numbers)
;|              (map #(map (fn [vm]
;|                           {:name (.getName vm)
;|                            :datastores (get-vm-datastores vm)
;|                            :IPs (get-ips [vm])
;|                            :power (power-states (-> vm .getRuntime .powerState))
;|                            :active-memory (-> vm .getSummary .quickStats .guestMemoryUsage)
;|                            :private-memory (-> vm .getSummary .quickStats .privateMemory)
;|                            :swapped-memory (-> vm .getSummary .quickStats .swappedMemory)
;|                            :ballooned-memory (-> vm .getSummary .quickStats .getBalloonedMemory)})
;|                         (vms-in-testbed %))
;|                   testbed-numbers))))
;|
;|(defn status
;|  "takes an optional collection of testbed number ie: (status [1 5 7]
;|to print a report on these three testbeds, or just (status) for all testbeds"
;|  ([] (status (range 1 41)))
;|  ([testbed-numbers]
;|     (let [report (report testbed-numbers)
;|           stats (map #(let [name (key %)
;|                             names (map :name (val %))
;|                             power-states (map :power (val %))
;|                             active-memory (map :active-memory (val %))
;|                             private-memory (map :private-memory (val %))
;|                             swapped-memory (map :swapped-memory (val %))
;|                             ballooned-memory (map :ballooned-memory (val %))
;|                             powerfn (fn [state]
;|                                       (filter (fn [power-state] (= state power-state))
;|                                               power-states))]
;|                         {:testbed name
;|                          :vms (count names)
;|                          :powered-on (count (powerfn :poweredOn))
;|                          :powered-off (count (powerfn :poweredOff))
;|                          :suspended (count (powerfn :suspended))
;|                          :active-memory (reduce + (filter (comp not nil?) active-memory))
;|                          :private-memory (reduce + (filter (comp not nil?) private-memory))
;|                          :swapped-memory (reduce + (filter (comp not nil?) swapped-memory))
;|                          :ballooned-memory (reduce + (filter (comp not nil?) ballooned-memory))})
;|                      report)
;|           sumfn (fn [key] (reduce + (filter (comp not nil?) (map key stats))))
;|           totals {:vms            (sumfn :vms)
;|                   :powered-on     (sumfn :powered-on)
;|                   :powered-off    (sumfn :powered-off)
;|                   :suspended      (sumfn :suspended)
;|                   :active-memory  (sumfn :active-memory)
;|                   :private-memory (sumfn :private-memory)
;|                   :swapped-memory (sumfn :swapped-memory)
;|                   :ballooned-memory (sumfn :ballooned-memory)}]
;|       {:totals totals
;|        :stats stats
;|        :report report})))
;|
;|(defn print-status [status]
;|  "prints the result of calling status"
;|  (println "Totals:")
;|  (pp/pprint (status :totals))
;|  (println "\nSummary")
;|  (pp/pprint (status :stats))
;|  (println "\n\nTestbed Details")
;|  (pp/pprint  (status :report)))


(def critical 2)
(def warning  1)
(def healthy  0)

(defn hosts-running [status]
  (if (= ((status :totals) :vms)
         (+ ((status :totals) :powered-off)
            ((status :totals) :suspended)))
    (do (println "no hosts powered on") critical)
    healthy))

(defn swapped-memory [status]
  (let [totals (status :totals)
        swapped-memory (totals :swapped-memory)]
    (if (> swapped-memory 0)
      (do (println "Memory Swapping: " swapped-memory "MB." ) critical)
      healthy)))

(defn ballooned-memory [status]
  (let [totals (status :totals)
        ballooned-memory (totals :ballooned-memory)]
    (if (> ballooned-memory 100)
      (do (println "Ballooned Memory " ballooned-memory) warning)
      healthy)))

(def tests [hosts-running swapped-memory ballooned-memory])
(defn analyze-status [status]
  (let [result (reduce max (map #(% status) tests))]
    (if (zero? result)
      (println (-> status :totals :vms) " active VMS"))
    result))


(comment

  (with-vc-login
        {:vc    "my.vc.company.com"
         :username "administrator"
         :password "sUpers3kret"}
        (with-vc-login
          (clone-vm "template-vm-name"
                    "new-vm-name"
                    "name-of-host-on-vcenter"
                    "my-datastore-name"
                    [["eth0" "Network1"] ["eth1" "Network2"]])))

)
