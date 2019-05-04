def try_decode(data):
    try:
        return data.decode('utf-8')
    except UnicodeDecodeError:
        return data
