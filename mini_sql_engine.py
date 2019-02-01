
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
    #print(identifier_list)
    get_data(identifier_list)


def get_data(iden_list):
    if(len(iden_list) >= 4):
        #Min query-"select * from table1 ;"

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
        if( tables_index >= len(iden_list)):
            error_msg("Error:Invalid Syntax")
        tables=iden_list[tables_index].split(',')
        # print(tables)

        #Reading attributes
        attributes=[]
        attributes=iden_list[1].split(',')
        #print(attributes)

        ##where clause
        conds=[]
        if("where" in iden_list[-1] or "WHERE" in iden_list[-1]):
            conds,and_flag,or_flag=process_where(iden_list[-1])

        #cond=['col=value','col<value','table1.col=table2.col,...]
        #conditions=[[0/1,col_no,op,value/col_no],[0/1,col_no,op,value/col_no],...]
        #0-col=val like [0,col_no,op,value], 1-table.col=table.col  like [1,col_no,op,col_no]
        conditions=[]
        op=""
        for condition in conds:
            condition=condition.strip(';')
            # print(condition)
            if( "<" in condition):
                op="<"
            elif( ">" in condition):
                op=">"
            elif( "<=" in condition):
                op="<="
            elif( ">=" in condition):
                op=">="
            elif( "=" in condition):
                op="=="

            if(op == "=="):
                col_name,part_2=condition.split('=')
            elif(op):
                col_name,part_2=condition.split(op)
            else:
                error_msg("Error:Invalid Syntax")

            col_no=get_col_no(tables,col_name)

            temp=[]
            if(part_2.isdigit()):
                value=part_2
                temp.append(0)
                temp.append(col_no)
                temp.append(op)
                temp.append(value)
                conditions.append(temp)
            else:
                col_no2=get_col_no(tables,part_2)
                temp.append(1)
                temp.append(col_no)
                temp.append(op)
                temp.append(col_no2)
                conditions.append(temp)
        #print(conditions)


        #If select '*'
        if(attributes[0] == '*'):
            if(len(tables) == 1):
                table=tables[0]
                for col in metadata[table]:
                    sys.stdout.write(col+'\t')
                sys.stdout.write('\n')
                file_name=FILES_PATH+table.strip()+".csv"
                file_data=read_csv_file(file_name)
                if(len(conds)== 0):
                    #No where condition
                    for row in file_data:
                        for i in row:
                            sys.stdout.write(i+'\t')
                        sys.stdout.write('\n')
                else:
                    #where condition
                    for row in file_data:
                        res,flag=check_cond(conditions,and_flag,or_flag,row)
                        if(res):
                            if(flag == 0):
                                for i in row:
                                    sys.stdout.write(i+'\t')
                                sys.stdout.write('\n')
                            else:
                                for i in row:
                                    if( i != col_no2 ):
                                        sys.stdout.write(i+'\t')
                                sys.stdout.write('\n')


            else:
                ##Join
                if(len(tables) != 2):
                    error_msg("Join is done for 2 tables only")
                table1=tables[0]
                table2=tables[1]
                file_name1=FILES_PATH+table1.strip()+".csv"
                file_data1=read_csv_file(file_name1)
                file_name2=FILES_PATH+table2.strip()+".csv"
                file_data2=read_csv_file(file_name2)

                column_names=[]
                for col in metadata[table1]:
                    col_name="table1"+"."+col
                    column_names.append(col_name)
                for col in metadata[table2]:
                    col_name="table2"+"."+col
                    column_names.append(col_name)

                ######
                # for col in column_names:
                #     sys.stdout.write(col+'  ')
                # sys.stdout.write('\n')
                
                file_data=[]
                for row1 in file_data1:
                    for row2 in file_data2:
                        file_data.append(row1+row2)
                        
                if(len(conds)== 0):
                    #No where condition
                    for row in file_data:
                        for i in row:
                            sys.stdout.write(i+'\t')
                        sys.stdout.write('\n')
                else:
                    #where condition
                    count = 0
                    for row in file_data:
                        res,flag=check_cond(conditions,and_flag,or_flag,row)
                        if(res):
                            if(flag == 0):
                                if(count == 0):
                                    for col in column_names:
                                        sys.stdout.write(col+'  ')
                                    sys.stdout.write('\n')
                                    count=1
                                for i in row:
                                    sys.stdout.write(i+'\t')
                                sys.stdout.write('\n')
                            else:
                                if(count == 0):
                                    for col in range(0,len(column_names)):
                                        if( col != col_no2):
                                            sys.stdout.write(column_names[col]+'  ')
                                    sys.stdout.write('\n')
                                    count=1
                                    
                                for i in range(0,len(row)):
                                    if( i != col_no2 ):
                                        sys.stdout.write(row[i]+'\t')
                                sys.stdout.write('\n')
                    
        ##Aggregate Functions- max,min,sum,avg
        elif(("max" in attributes[0]) or ("min" in attributes[0]) or ("avg" in attributes[0]) or ("sum" in attributes[0])):
            #Only one table
            if(len(tables) > 1):
                msg="Error: Invalid Syntax - Aggregate Functions can be applied on only one table"
                error_msg(msg)
            if (len(tables)==1):
                table = tables[0].strip()
            #Only on 1 column
            temp_col=attributes[0].split('(')[1]
            if(temp_col[-1] != ')'):
                msg="Error: Invalid Syntax"
                error_msg(msg)    
            col=temp_col[0:-1]

            if col not in metadata[table]:
                msg="Error: attribute doesn't exist"
                error_msg(msg)  
            else:
                col_no=metadata[table].index(col)

            if("max" in attributes[0]):
                print("max(%s.%s)" %(table,col))
                file_name=FILES_PATH+table.strip()+".csv"
                file_data=read_csv_file(file_name)
                max=-sys.maxsize
                for row in file_data:
                    if(max < int(row[col_no])):
                        max=int(row[col_no])
                print(max,"\n")

            elif("min" in attributes[0]):
                print("min(%s.%s)" %(table,col))
                file_name=FILES_PATH+table.strip()+".csv"
                file_data=read_csv_file(file_name)
                min=sys.maxsize
                for row in file_data:
                    if(min > int(row[col_no])):
                        min=int(row[col_no])
                print(min,"\n")

            elif("sum" in attributes[0]):
                print("sum(%s.%s)" %(table,col))
                file_name=FILES_PATH+table.strip()+".csv"
                file_data=read_csv_file(file_name)
                sum=0
                for row in file_data:
                    sum+=int(row[col_no])
                print(sum,"\n")

            elif("avg" in attributes[0]):
                print("avg(%s.%s)" %(table,col))
                file_name=FILES_PATH+table.strip()+".csv"
                file_data=read_csv_file(file_name)
                sum=0
                count=0
                avg=0
                for row in file_data:
                    sum+=int(row[col_no])
                    count+=1
                if(count != 0):
                    avg=sum/count
                print(avg,"\n")

        ##Distinct
        elif( attributes[0].strip() == "distinct" ):
            #Only one table
            if(len(tables) > 1):
                msg="Error: Invalid Syntax - distinct can be applied on only one table"
                error_msg(msg)
            if (len(tables)==1):
                table = tables[0].strip()

            attrs=iden_list[2].split(',')
            # print(attrs)
            column_nos = []
            for i in attrs:
                print("distinct(%s)\t"%i)
                if i in metadata[table]:
                    column_nos.append(metadata[table].index(i))
                else:
                    msg="Error: attribute doesn't exist"
                    error_msg(msg)
            print("\n")
            file_name=FILES_PATH+table.strip()+".csv"
            file_data=read_csv_file(file_name)
            distinct_values=[]
            for row in file_data:
                row_values=[]
                for i in column_nos:
                    row_values.append(row[i])
                if( row_values not in distinct_values):
                    distinct_values.append(row_values)
            # print(distinct_values)
            for row in distinct_values:
                for i in list(row):
                    sys.stdout.write(i+'\t\t')
                sys.stdout.write('\n')

        #Normal query- select A,B ...
        else:
            if (len(tables)==1):
                table = tables[0]
                col_nos = []
                for i in attributes:
                    sys.stdout.write(i+'\t')
                    if i in metadata[table]:
                        col_nos.append(metadata[table].index(i))
                    else:
                        msg="Error: attribute doesn't exist"
                        error_msg(msg)
                sys.stdout.write('\n')
                file_name=FILES_PATH+table.strip()+".csv"
                file_data=read_csv_file(file_name)
                if(len(conds)== 0):
                    #No where condition
                    for row in file_data:
                        for i in col_nos:
                            sys.stdout.write(row[i]+'\t')
                        sys.stdout.write('\n')
                else:
                    #where condition
                    for row in file_data:
                        res,flag=check_cond(conditions,and_flag,or_flag,row)
                        if(res):
                            if(flag == 0):
                                for i in col_nos:
                                    sys.stdout.write(row[i]+'\t')
                                sys.stdout.write('\n')
                            else:
                                for i in col_nos:
                                    if( i != col_no2 ):
                                        sys.stdout.write(row[i]+'\t')
                                sys.stdout.write('\n')

            else:
                ##Join
                if(len(tables) > 2):
                    error_msg("Join is done for 2 tables only")

                table1=tables[0]
                table2=tables[1]
                file_name1=FILES_PATH+table1.strip()+".csv"
                file_data1=read_csv_file(file_name1)
                file_name2=FILES_PATH+table2.strip()+".csv"
                file_data2=read_csv_file(file_name2)

                column_names=[]
                for col in metadata[table1]:
                    col_name="table1"+"."+col
                    column_names.append(col_name)
                for col in metadata[table2]:
                    col_name="table2"+"."+col
                    column_names.append(col_name)

                len_table1=len(metadata[table1])

                #Which cols to print
                col_nos=[]
                table=None
                for a_i in range(0,len(attributes)):
                    found=0
                    if ( "." in attributes[a_i]):
                        table,col=attributes[a_i].split('.')
                        col_no=metadata[table].index(col)
                    else:
                        for i in range(0,len(tables)):
                            if(attributes[a_i] in metadata[tables[i]]):
                                table=tables[i]
                                col=attributes[a_i]
                                col_no=metadata[table].index(col)
                                if(found == 1):
                                    msg=("Error:Ambiguous attribute %s"%attributes[a_i])
                                    error_msg(msg)
                                found=1
                    if(table == None):
                        error_msg("Error:Invalid Syntax")

                    if(table == table1):
                        col_nos.append(col_no)
                    elif(table == table2):
                        col_nos.append(len_table1+col_no)

                for col in col_nos:
                    sys.stdout.write(column_names[col]+'  ')
                sys.stdout.write('\n')
                
                file_data=[]
                for row1 in file_data1:
                    for row2 in file_data2:
                        file_data.append(row1+row2)
                        
                if(len(conds)== 0):
                    #No where condition
                    for row in file_data:
                        for i in col_nos:
                            sys.stdout.write(row[i]+'\t')
                        sys.stdout.write('\n')
                else:
                    #where condition
                    for row in file_data:
                        res,flag=check_cond(conditions,and_flag,or_flag,row)
                        if(res):
                            if(flag == 0):
                                for i in col_nos:
                                    sys.stdout.write(row[i]+'\t')
                                sys.stdout.write('\n')
                            else:
                                for i in col_nos:
                                    if( i != col_no2):
                                        sys.stdout.write(row[i]+'\t')
                                sys.stdout.write('\n')
                
    else:
        msg="Error:Invalid Syntax(length)"
        error_msg(msg)


def get_col_no(tables,col_name):
    #col_no of given attribute in condition
    col_no=0
    found=0
    if ( "." in col_name):
        table,col=col_name.split('.')
        col_no=metadata[table].index(col)
    else:
        for i in range(0,len(tables)):
            if(col_name in metadata[tables[i]]):
                table=tables[i]
                col=col_name
                col_no=metadata[table].index(col)
                if(found == 1):
                    msg=("Error:Ambiguous attribute %s"%col_name)
                    error_msg(msg)
                found=1
    #If join (col_no changes)- only 2 tables
    if(len(tables)>1):
        table1=tables[0]
        table2=tables[1]
        len_table1=len(metadata[table1])
        if(table == table2):
            col_no=len_table1+col_no
    return col_no



def check_cond(conditions,and_flag,or_flag,row):
    res=True
    ans=[]
    flag=None  #if join cond- to not print repeated columns
    for condition in conditions:
        if(condition[0] == 0):
            col_no=condition[1]
            op=condition[2]
            value=condition[3]
            ans.append(eval(row[col_no]+op+value))
            if( not flag ):
                flag=0
        if(condition[0] == 1):
            col_no=condition[1]
            op=condition[2]
            col_no2=condition[3]
            ans.append(eval(row[col_no]+op+row[col_no2]))
            flag=1
    if(and_flag):
        res=ans[0]
        for a in range(1,len(ans)):
            res= res and ans[a]
    elif(or_flag):
        res=ans[0]
        for a in range(1,len(ans)):
            res= res or ans[a]
    else:
        res=ans[0]
    # print(res)
    return res,flag
        

def process_where(where_cond):
    cond_list=where_cond.split(" ")
    #Maximum one operator (2 conditions)
    conds=[]
    and_flag=0
    or_flag=0

    for ele in cond_list:
        if( ele == "where" or ele == "WHERE" or ele == ';'):
            continue
        elif( ele == 'and' or ele == 'AND'):
            and_flag=1
        elif( ele == 'or' or ele == 'OR'):
            or_flag=1
        elif( ele == 'not' or ele == 'NOT'):
            msg="Error:not is not allowed"
            error_msg(msg)
        else:
            conds.append(ele)

    if( and_flag and or_flag):
        msg="Error:one and/or operator is allowed"
        error_msg(msg)
    # print(conds)
    return conds,and_flag,or_flag



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
