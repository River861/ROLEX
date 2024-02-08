# ROLEX

## Environment Setup


1) Set bash as the default shell. And enter the ROLEX directory.
    ```shell
    sudo su
    chsh -s /bin/bash
    cd ROLEX
    ```

2) Install Mellanox OFED.
    ```shell
    # It doesn't matter to see "Failed to update Firmware"
    # This takes about 8 minutes
    sh ./script/installMLNX.sh
    ```

3) Resize disk partition.

    Since the r650 nodes remain a large unallocated disk partition by default, you should resize the disk partition using the following command:
    ```shell
    # It doesn't matter to see "Failed to remove partition" or "Failed to update system information"
    sh ./script/resizePartition.sh
    # This takes about 6 minutes
    reboot
    # After rebooting, log into all nodes again and execute:
    sudo su
    resize2fs /dev/sda1
    ```

4) Enter the directory. Install libraries and tools.
    ```shell
    cd ROLEX
    # This takes about 3 minutes
    sh ./script/installLibs.sh
    ```


## YCSB Workloads

You should run the following steps on **all** nodes.

1) Download YCSB source code.
    ```shell
    sudo su
    cd ROLEX/ycsb
    curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.11.0/ycsb-0.11.0.tar.gz
    tar xfvz ycsb-0.11.0.tar.gz
    mv ycsb-0.11.0 YCSB
    ```
2) Download the email dataset for string workloads.
    ```shell
    gdown --id 1ZJcQOuFI7IpAG6ZBgXwhjEeKO1T7Alzp
    ```

3) We first generate a small set of YCSB workloads here for **quick start**.
    ```shell
    # This takes about 2 minutes
    sh generate_small_workloads.sh
    ```


## Getting Started *(Artifacts Functional)*

* HugePages setting.
    ```shell
    sudo su
    echo 36864 > /proc/sys/vm/nr_hugepages
    ulimit -l unlimited
    ```

* Return to the root directory and execute the following commands on **all** nodes to compile ROLEX:
    ```shell
    mkdir build; cd build; cmake ..; make -j
    ```

* Execute the following command on **one** node to initialize the memcached:
    ```shell
    /bin/bash ../script/restartMemc.sh
    ```

* Execute the following command on **all** nodes to split the workloads:
    ```shell
    python3 ../ycsb/split_workload.py <workload_name> <key_type> <CN_num> <client_num_per_CN>
    ```
    * workload_name: the name of the workload to test (*e.g.*, `a` / `b` / `c` / `d` / `la`).
    * key_type: the type of key to test (*i.e.*, `randint` / `email`).
    * CN_num: the number of CNs.
    * client_num_per_CN: the number of clients in each CN.

    **Example**:
    ```shell
    python3 ../ycsb/split_workload.py a randint 16 24
    ```

* Execute the following command in **all** nodes to conduct a YCSB evaluation:
    ```shell
    ./ycsb_test <CN_num> <client_num_per_CN> <coro_num_per_client> <key_type> <workload_name>
    ```
    * coro_num_per_client: the number of coroutine in each client (2 is recommended).

    **Example**:
    ```shell
    ./ycsb_test 16 24 2 randint a
    ```

* Results:
    * Throughput: the throughput of **ROLEX** among all the cluster will be shown in the terminal of the first node (with 10 epoches by default).
    * Latency: execute the following command in **one** node to calculate the latency results of the whole cluster:
        ```shell
        python3 ../us_lat/cluster_latency.py <CN_num> <epoch_start> <epoch_num>
        ```

        **Example**:
        ```shell
        python3 ../us_lat/cluster_latency.py 16 1 10
        ```
