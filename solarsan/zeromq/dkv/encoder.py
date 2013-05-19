

class SimpleEncoder(object):
    '''
    An in-process "encoder" that is primarily useful for unit testing.
    '''
    def encode(self, node_uid, message_type, parts):
        return ['{0}\0{1}'.format(node_uid, message_type)] + list(parts)

    def decode(self, parts):
        from_uid, message_type = parts[0].split('\0')
        return from_uid, message_type, parts[1:]


def _decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = _decode_list(item)
        elif isinstance(item, dict):
            item = _decode_dict(item)
        rv.append(item)
    return rv


def _decode_dict(data):
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = _decode_list(value)
        elif isinstance(value, dict):
            value = _decode_dict(value)
        rv[key] = value
    return rv


try:
    import ujson

    class UJSONEncoder (object):
        def encode(self, node_uid, message_type, parts):
            header = dict()

            header['type'] = message_type
            header['from'] = node_uid

            plist = [header]

            if parts:
                plist.extend(parts)

            return [str(ujson.dumps(p)) for p in plist]

        def decode(self, jparts):
            if not jparts:
                return

            try:
                parts = [ujson.loads(j) for j in jparts]
            except ValueError:
                print 'Invalid JSON: ', jparts
                return

            header = parts[0]
            parts = parts[1:]

            return header['from'], header['type'], parts
except ImportError:
    pass


import json


class JSONEncoder (object):
    def encode(self, node_uid, message_type, parts):
        header = dict()

        header['type'] = message_type
        header['from'] = node_uid

        plist = [header]

        if parts:
            plist.extend(parts)

        return [str(json.dumps(p)) for p in plist]

    def decode(self, jparts):
        if not jparts:
            return

        try:
            parts = [json.loads(j, object_hook=_decode_dict) for j in jparts]
        except ValueError:
            print 'Invalid JSON: ', jparts
            return

        header = parts[0]
        parts = parts[1:]

        return header['from'], header['type'], parts
