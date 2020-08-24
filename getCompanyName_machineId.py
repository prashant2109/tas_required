import os, sys

# to get machineId and companyName    
def getCN_MID():
    cnmid_txtPath  = '/mnt/eMB_db/dealid_company_info.txt'
    f = open(cnmid_txtPath, 'r') 
    data_txt = f.readlines()
    f.close()
    getCNMID = {}
    for cnmid in data_txt:
        company_id, company_name, ipaddr = cnmid.strip().split(':$$:')
        getCNMID[company_id] = (company_name, ipaddr)
    return getCNMID

# to get only IP
def getCN_MIP():
    cnmid_txtPath  = '/mnt/eMB_db/dealid_company_info.txt'
    f = open(cnmid_txtPath, 'r')
    data_txt = f.readlines()
    f.close()
    getCNMIP = {}
    for cnmid in data_txt:
        company_id, company_name, ipaddr = cnmid.strip().split(':$$:')
        getCNMID[company_id] = ipaddr
    return getCNMIP



