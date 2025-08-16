

capacities = {}

def handleCapacity(response, id):
    # if id already exists, update it
    if id in capacities:
        capacities[id] += response['ConsumedCapacity']['CapacityUnits']
    else:
        capacities[id] = response['ConsumedCapacity']['CapacityUnits']

def printCapacities():
    for id, capacity in capacities.items():
        print(f"{id}: {capacity} units")
    
    #total:
    total_capacity = sum(capacities.values())
    print(f"Total Capacity Consumed: {total_capacity} units")