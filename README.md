# EECS6432Project

How to read excel sheets
Observations are written as:
sql_cpu_avg WW_cpu_avg sql_mem_avg web_worker_mem_avg minutes_since_start seconds_since_start

Results are written as:
sql_cpu_pred WW_cpu_pred sql_mem_pred web_worker_mem_pred sql_containers_count WW_containers_count delta_requests iteration minutes_since_start seconds_since_start 

# To run software:
python main.py --node_list list of IP addresses for docker quieries (typically nodeIP address port 4000) --req_list list of IP addresses to send requests to (typically nodeIP address port 80) --manager IP address of the manager node (typically nodeIP address port 4000) --number_loads number of load processes --poll_interval time in seconds between estimations --polls_per_update number of estimates before the system updates parameters --log_file path to file where results are stored
