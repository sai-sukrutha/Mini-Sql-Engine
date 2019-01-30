
import sys
import csv
import os.path
from os import path
import sqlparse

FILES_PATH="./sample_files/"
METADATA_FILE_PATH="./sample_files/metadata.txt"

metadata={}

def read_input(args):
    if(len(args) < 2):
        msg=('Error:Please provide query : python %s "query"' %sys.argv[0])
        error_msg(msg)
    #Reading metadata.txt into dictionary
    read_metadata()
    #check query
    parse_query(args[1])
    # read_csv_file(args[1])


def read_metadata():
    if(os.path.isfile(METADATA_FILE_PATH)):
        meta_file=open(METADATA_FILE_PATH,'r')
        flag=0
        for line in meta_file:
            if(line.strip() == "<begin_table>"):
                flag=1
                continue
            if(flag == 1):
                table_name=line.strip()
                metadata[table_name]=[]
                flag=0
                continue
            if(line.strip() != "<end_table>"):
                metadata[table_name].append(line.strip())
    else:
        msg=('Error:Metadata file is not found')
        error_msg(msg)


def parse_query(query):
    if(query[-1] != ";"):
        msg="Syntax Error:Semicolon missing"
        error_msg(msg)
    parsed_query=sqlparse.parse(query)
    sql_query=parsed_query[0]
    if(sql_query.get_type() != "SELECT"):
        msg="Error:Cannot process queries other than SELECT"
        error_msg(msg)
    all_list= sqlparse.sql.IdentifierList(sql_query).get_identifiers()
    identifier_list = []
    for ele in all_list:
        identifier_list.append(str(ele))
    # print(identifier_list)
    get_data(identifier_list)


def get_data(iden_list):
    if(len(iden_list) >= 5):
        #Min query-"select * from table1 ;"
        ##TODO

        #Reading table name ( tablename after from )
        from_index=2
        if( "from" in iden_list):
            from_index=iden_list.index("from")
        elif( "FROM" in iden_list):
            from_index=iden_list.index("FROM")
        else:
            msg="Invalid Syntax"
            error_msg(msg)

        tables_index=from_index+1
        tables=[]
        file_data=[]
        tables=iden_list[tables_index].split(',')
        # print(tables)

        #Reading attributes
        attributes=[]
        attributes=iden_list[1].split(',')

        ###TODO: from table1,table2 - done as just printing but actually join

        if(attributes[0] == '*'):
            for i in range(0,len(tables)):
                for col in metadata[tables[i]]:
                    print(col+'\t',end='')
                print('\n')

                file_name=FILES_PATH+tables[i].strip()+".csv"
                file_data=read_csv_file(file_name)
                for row in file_data:
                    for i in row:
                        print(i+'\t',end='')
                    print('\n')
                    
        ##TODO:Aggregate Functions
        # elif("max")       
        else:
            if (len(tables)==1):
                table = tables[0]
                column_nos = []
                for i in attributes:
                    print(i+'\t',end='')
                    if i in metadata[table]:
                        column_nos.append(metadata[table].index(i))
                    else:
                        msg="Error: attribute doesn't exist"
                        error_msg(msg)
                print('\n')
                file_name=FILES_PATH+table.strip()+".csv"
                file_data=read_csv_file(file_name)
                for row in file_data:
                    for i in column_nos:
                        print(row[i]+'\t',end='')
                    print('\n')
            else:
                for j in range(0,len(tables)):
                    table = tables[j]
                    column_nos = []
                    for i in attributes:
                        print(i+'\t',end='')
                        if i in metadata[table]:
                            column_nos.append(metadata[table].index(i))
                        else:
                            msg="Error: attribute doesn't exist"
                            error_msg(msg)
                    print('\n')
                    file_name=FILES_PATH+tables[j].strip()+".csv"
                    file_data=read_csv_file(file_name)

                    for row in file_data:
                        for i in column_nos:
                            print(row[i]+'\t',end='')
                        print('\n')

    else:
        msg="Invalid Syntax"
        error_msg(msg)


def read_csv_file(file_path):
    if(os.path.isfile(file_path)):
        file_data=[]
        with open(file_path, mode='r') as csv_file:
            file_reader = csv.reader(csv_file, delimiter=',')
            for row in file_reader:
                file_data.append(row)
            return file_data
    else:
        msg=('Error:Table does not exist in given path : %s ' %file_path)
        error_msg(msg)
    return file_data


def error_msg(msg):
    print(msg)
    exit()


def main(args):
    read_input(args)
    return


if __name__ == "__main__":
   main(sys.argv)
