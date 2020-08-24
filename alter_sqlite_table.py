import os, sys, sqlite3 

def alter_table_sqlite3(conn, cur, table_name, list_columns):
    prag_qry = 'pragma table_info(%s);'%(table_name)
    cur.execute(prag_qry)
    table_data = cur.fetchall()
    get_all_existing_columns = [str(col[1]) for col in table_data]   
    new_columns = set(list_columns) - set(get_all_existing_columns)
    for colm in list_columns:
        if colm not in new_columns:continue
        add_qry = ' '.join([colm, 'TEXT'])
        alter_table_qry = 'ALTER TABLE %s ADD COLUMN %s'%(add_qry)
        cur.execute(alter_table_qry) 
    conn.commit()
    conn.close()
    return 'done'








