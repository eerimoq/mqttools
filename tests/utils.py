async def get_broker_address(broker):
    # IPv6 gives 4-tuple, need only host and port.
    address_tuple = await broker.getsockname()

    return address_tuple[:2]
