from ..common import hexlify


def format_message(kind, message):
    if kind == 'auto':
        try:
            message = message.decode()
        except UnicodeDecodeError:
            message = hexlify(message)
    elif kind == 'binary':
        message = hexlify(message)
    elif kind == 'text':
        message = message.decode(errors='replace')
    else:
        message = ''

    return message.replace('\x00', '\\x00')
