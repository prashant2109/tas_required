import os, sys
import copy
import math
import base64
import time
import lmdb
import binascii
import ast
import json
import shutil
import cPickle
import shelve
import Crypto.Hash.SHA256 as SHA256

class webdatastore(object):
    def __init__(self):
        return

    def make_dirs(self, dirname):
        dirname = os.path.join(dirname, '')
        if dirname and (not os.path.exists(dirname)):
            os.makedirs(dirname)
        return

    def rmdir(self, dirname):
        try:
            if os.path.isdir(dirname):
                shutil.rmtree(dirname)
            elif os.path.isfile(dirname):
                os.remove(dirname)
        except OSError, (errno, strerror):
            errmsg = 'Error removing %s, %s' %(dirname, str(strerror))
            #print errmsg
        return

    def rmfile(self, fname):
        if os.path.isfile(fname):
            os.remove(fname)
        return

    def __get_hash_key(self, msg):
        return SHA256.new(msg).hexdigest()

    ##-----------------------------------------------------------------------


    def __write_to_shelve(self, fname, idata):
        dirname = os.path.dirname(fname)
        if dirname and (not os.path.exists(dirname)):
            os.makedirs(dirname)
        sh = shelve.open(fname, 'n')
        sh['data'] = idata
        sh.close()
        return

    def __read_from_shelve(self, fname, default={}):
        if os.path.isfile(fname):
            sh = shelve.open(fname, 'r')
            data = sh.get('data', default)
            sh.close()
            return data
        return default

    ##-----------------------------------------------------------------------


    def __write_to_lmdb_old(self, fname, idata):
        if not os.path.exists(fname):
            os.makedirs(fname)
        elif os.path.isdir(fname):
            shutil.rmtree(fname)
        else:
            os.remove(fname)

        env = lmdb.open(fname, map_size=20*1024*1024*1024)
        with env.begin(write=True) as txn:   
            jdata = repr(idata)
            data = binascii.b2a_base64(jdata)
            txn.put("data", data)
        return

    def __read_from_lmdb_old(self, fname, default=None):
        if os.path.exists(fname) and os.path.isdir(fname):
            env = lmdb.open(fname)
            with env.begin() as txn:  
                data = txn.get('data')
                if data:
                    jdata = binascii.a2b_base64(data)
                    data = ast.literal_eval(jdata)
                    return data
        return default

    def __remove_lmdb(self, fname):
        retval = 0
        if os.path.isdir(fname):
            shutil.rmtree(fname)
        elif os.path.isfile(fname):
            os.remove(fname)
        return retval

    def __reset_lmdb(self, fname):
        retval = 0
        if os.path.isdir(fname):
            shutil.rmtree(fname)
        elif os.path.isfile(fname):
            os.remove(fname)
        if not os.path.exists(fname):
            os.makedirs(fname)
        return retval

    def __encode_data(self, idata):
        pdata = cPickle.dumps(idata)
        encmsg = base64.b64encode(pdata)
        #print 'cdata:', [idata, pdata, encmsg, 123]
        return encmsg

    def __decode_data(self, encmsg):
        decmsg = base64.b64decode(encmsg)
        idata = cPickle.loads(str(decmsg))
        return idata

    def split_dict_equally_old(self,input_dict):
        "Splits dict by keys. Returns a list of dictionaries."
        chunks_size = int(math.ceil(sys.getsizeof(input_dict)/12583050))
        if chunks_size <= 0:
            chunks_size = 1
        #print 'chunks_size',chunks_size,sys.getsizeof(input_dict)
        # prep with empty dicts
        return_list = [dict() for idx in xrange(chunks_size)]
        idx = 0
        for k,v in input_dict.iteritems():
            return_list[idx][k] = v
            if idx < chunks_size-1: # indexes start at 0
                idx += 1
            else:
                idx = 0
        return return_list

    def split_dict_equally(self, idata, ssize=64):
        split_size = ssize * 1024 # KB
        split_size = split_size - 32

        retlst = []

        tmpd = {}
        tmp_kvsize = 0
        total_kvsize = 0
        for k, v in idata.items():
            kvsize = len(k) + len(v) + 8
            if (tmp_kvsize + kvsize) >= split_size:
                tmp_kvsize = 0
                if tmpd:
                    retlst.append(tmpd)
                tmpd = {}
            tmpd[k] = v
            tmp_kvsize += kvsize
            total_kvsize += kvsize
        if tmpd:
            retlst.append(tmpd)
        #print len(retlst), len(idata), (total_kvsize/1024.0)
        return retlst

    def split_dict_equallyv2(self, idata, ssize=1000000):
        retlst = []

        tmpd = {}
        cnt = 0
        all_keys = idata.keys()
        all_keys.sort()
        for k in all_keys:
            if cnt > ssize:
                retlst.append(tmpd)
                cnt = 0
                tmpd = {}
            tmpd[k] = idata[k]
            cnt += 1

        if tmpd:
            retlst.append(tmpd)

        return retlst

    def __write_to_lmdb(self, fname, idata, wkeylst, nflag=0):
        #print 'coming here....'
        #t0 = time.time()
        retval = 0
        if nflag:
            self.__reset_lmdb(fname)
        else:
            self.make_dirs(fname)

        all_lmdb_keys = self.__read_from_lmdb_keys(fname, {})

        lmdb_info = {}
        for ikey in wkeylst:
            enckey = self.__encode_data(ikey)
            #hash_key = self.__get_hash_key(ikey)
            hash_key = self.__get_hash_key(enckey)
            all_lmdb_keys[ikey] = hash_key
            #print 'Write:', [ikey, enckey, hash_key]

            ival = idata.get(ikey, None)        # value data
            encval = self.__encode_data(ival)

            lmdbkey = 'val:'+hash_key
            lmdb_info[lmdbkey] = encval

        idbkey = 'all_lmdb_keys_lmdb_keys'
        enckey = self.__encode_data(idbkey)
        hash_key = self.__get_hash_key(enckey)
        lmdbkey = 'val:'+hash_key
        lmdb_info[lmdbkey] = self.__encode_data(all_lmdb_keys)
        t1 = time.time()
        #print 't1:', t1-t0

        lmdb_data_lst = []
        if 1:
            t2 = time.time()
            lmdb_data_lst = self.split_dict_equally(lmdb_info)
            t3 = time.time()
            #print 'len:', len(lmdb_data_lst), t3-t2
            #print t3-t0
            #sys.exit()
            for each in lmdb_data_lst:
                env = lmdb.open(fname, map_size=20*1024*1024*1024)
                with env.begin(write=True) as txn:
                    for k, v in each.items():
                        txn.put(k, v)
                env.close()

        if 0:
            for k, v in lmdb_info.items():
                env = lmdb.open(fname, map_size=20*1024*1024*1024)
                with env.begin(write=True) as txn:
                    txn.put(k, v)
 
        if 0:
            env = lmdb.open(fname, map_size=20*1024*1024*1024)
            with env.begin(write=True) as txn:
                for ikey in wkeylst:
                    enckey = self.__encode_data(ikey)
                    hash_key = self.__get_hash_key(ikey)

                    ival = idata.get(ikey, None)        # value data
                    encval = self.__encode_data(ival)

                    txn.put('key:'+hash_key, b2a_ikey)
                    txn.put('val:'+hash_key, encval)


        if os.path.exists(fname):
            cmd = "chown -R apache:apache %s" %(fname)
            os.system(cmd)
            #cmd1 = "chmod -R 777 %s" %(fname)
            #os.system(cmd1) 
        return retval

    def __read_from_lmdb(self, fname, rkeylst, default={}):
        read_data = {}
        if os.path.exists(fname) and os.path.isdir(fname):
            env = lmdb.open(fname)
            with env.begin() as txn:
                for ikey in rkeylst:
                    #hash_key = self.__get_hash_key(ikey)
                    enckey = self.__encode_data(ikey)
                    hash_key = self.__get_hash_key(enckey)
                    #print 'Read:', [ikey, enckey, hash_key]

                    ival = None
                    encval = txn.get('val:'+hash_key)
                    if encval:
                        ival = self.__decode_data(encval)
                        read_data[ikey] = ival
            return read_data
        return default

    def __read_from_lmdbkeyinfo(self, fname, rkeyinfo, default={}):
        read_data = {}
        if os.path.exists(fname) and os.path.isdir(fname):
            env = lmdb.open(fname)
            with env.begin() as txn:
                for ikey, hash_key in rkeyinfo.items():

                    ival = None
                    encval = txn.get('val:'+hash_key)
                    if encval:
                        ival = self.__decode_data(encval)
                        read_data[ikey] = ival
            return read_data
        return default
 
    def __read_from_lmdb_keys(self, fname, default={}):
        tmpd = self.__read_from_lmdb(fname, ['all_lmdb_keys_lmdb_keys'], {})
        all_lmdb_keys = tmpd.get('all_lmdb_keys_lmdb_keys', default)
        return all_lmdb_keys

    def __remove_from_lmdb_keys(self, fname, rkeys):
        retval = 0
        all_lmdb_keys = self.__read_from_lmdb(fname, ['all_lmdb_keys_lmdb_keys'], default)
        return retval

    ##-----------------------------------------------------------------------


    def write_to_shelve(self, fname, data):
        self.__write_to_shelve(fname, data)
        return

    def read_from_shelve(self, fname, default={}):
        return self.__read_from_shelve(fname, default)

    def get_hash_key(self, msg):
        return self.__get_hash_key(msg)

    def encode_data(self, idata):
        return self.__encode_data(idata)

    def decode_data(self, encmsg):
        return self.__decode_data(encmsg)

    def write_to_lmdb(self, fname, idata, ikeys, nflag=0):
        self.__write_to_lmdb(fname, idata, ikeys, nflag)
        return

    def read_from_lmdb_key(self, fname, rkey, default={}):
        rkeys = [rkey]
        return self.__read_from_lmdb(fname, rkeys, default).get(rkey, {})

    #def read_from_lmdb_multiple_keys(self, fname, rkeys, default={}):
    #    return self.__read_from_lmdb(fname, rkeys, default)#.get(rkey, {})

    def read_from_lmdb(self, fname, rkeys, default={}):
        return self.__read_from_lmdb(fname, rkeys, default)

    def read_from_lmdb_keys(self, fname, default={}):
        return self.__read_from_lmdb_keys(fname, default)

    def read_all_from_lmdb(self, fname, default={}):
        dbkeyinfo = self.read_from_lmdb_keys(fname, default)
        return self.__read_from_lmdbkeyinfo(fname, dbkeyinfo, default)

    def remove_from_lmdb_keys(self, fname, rkeys):
        return self.__remove_from_lmdb_keys(fname, rkeys)

    def reset_lmdb(self, fname):
        self.__reset_lmdb(fname)

    def rmlmdb(self, fname):
        self.__remove_lmdb(fname)

    def debug(self):
        return

