from socketIO_client import SocketIO, LoggingNamespace
import sys, os, datetime
import html_entity_to_single_char
e_obj   = html_entity_to_single_char.html_entity_to_single_char()
class socket_client_utils2(object):
    def __init__(self, host, port_no):
        self.socket = self._connect(host, port_no)
        pass

    def _connect(self, host, port_no):
        socket = SocketIO(host, port_no, LoggingNamespace, transports=['websocket', 'xhr-polling'])
        #socket = SocketIO(host, port_no, LoggingNamespace)
        return socket

    def _on_response(self, args):
        self.data = args
        self.done = True
        return

    def isConnected(self):
        if self.socket and self.socket.connected:
            return True
        return False

    def send(self, data):
        self.data = {};
        if self.isConnected():
            self.socket.emit('data', data)
            self.socket.on('data', self._on_response)
            self.done = False;
            while not self.done:
                self.socket.wait(1);
        return self.data

    def send2(self, data, html_flg='TEXT', from_lang='',timeout=10):
        if isinstance(data, list):
            pass
        else:
            if os.path.exists(data):
                data    = open(data, 'r').read()
            data    = [data]
        tmpdata = []
        for t in data:
            if isinstance(t, unicode):
                t    = t.encode('utf-8')
                
            try:
                t    = t.decode('utf-8')
            except:pass
            t   = e_obj.convert(t)
            tmpdata.append(t)
        data    = tmpdata
        #print data
        #datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


        #room_name  = 'Client_'+datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        room_name  = 'Client_'+self.socket._engineIO_session.id+'_'+datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        emit_key    = 'text_translate' 
        on_key      = room_name+'_caller'
        o_data = {'res':{}};
        def on_response(args):
            o_data['res']       = args
            o_data['done']      = True
        def rm_func(args):
            pass
        if self.isConnected():
            self.socket.emit(emit_key, {'txts':data,'callback':room_name,'html':html_flg, 'from_lang':from_lang})
            self.socket.once(on_key, on_response)
            o_data['done'] = False;
            tcnt = 0
            while not o_data['done']:
                self.socket.wait(0.1);
                if ((timeout > 0) and (tcnt > timeout)):
                    break
                tcnt += 0.1
            self.socket.off(on_key)
        return o_data['res']

    def disconnect(self):
        if self.isConnected():
            self.socket.disconnect()
        return

    def close(self):
        self.disconnect()
        self.socket = None
        return

    def debug(self):
        return


if __name__ == '__main__':
    obj = socket_client_utils2(sys.argv[1], sys.argv[2])
    from_lang = ''
    if(len(sys.argv) > 4):
        from_lang   = sys.argv[4]
    output  = obj.send2(sys.argv[3], '', from_lang, 5)
    obj.close()
    print 'Input  ', [sys.argv[3]]
    print 'Output ', [output]
