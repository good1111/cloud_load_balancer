
# Scheduler of server
def scheduler(main_receiver,agent_max_host=20):
    original_node_list = main_receiver.get_node_map
    
    if not original_node_list:  # if node list is empty, just return False
        return False
    
    temp_host_amount = {}
    temp_cpu_load = {}
    
    for node in original_node_list:
        node_id = node['node_id']
        host_amount = node['host_amount']
        total_host = host_amount['http_host']  + host_amount['tcp_host']
        fifteen_cpu_load_avg = node['sysinfo']['load_avg'][2]
        
        if total_host <= agent_max_host:        # if the host amount of agent more than 20, ignore this agent
            temp_host_amount[node_id] = total_host
            temp_cpu_load[node_id] = fifteen_cpu_load_avg
    
    min_host_amount_list = []
    min_cpu_load_list = []
    
    min_host_amount = min(temp_host_amount.values())    # the minimum host amount.
    
    for host,value in temp_host_amount.items():
        if min_host_amount == value:                    # if a agent which has the minimum host amount.
            min_host_amount_list.append(host)
    
    if len(min_host_amount_list) == 1:              # found only one host that the host amount is minimum
        return min_host_amount_list[0]
    else:                                           # found more than one host, that means the host amount of them are equaly.
        min_cpu_load = min(temp_cpu_load.values())  # so next we must check the cpu load of every agent as same as the above.
        for host,value in temp_cpu_load.items():
            if min_cpu_load == value:
                min_cpu_load_list.append(host)
                
        if len(min_cpu_load_list) == 1:
            return min_cpu_load_list[0]
        else:
            return min_cpu_load_list.pop()
    return False    # we can not got any node, so must return False at last.

