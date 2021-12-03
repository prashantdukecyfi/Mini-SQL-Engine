#!/usr/bin/env python3
import sys
import sqlparse

schema={}
database={}

def clean(s):
	s=s.strip()
	while len(s)>1 and (s[0]=='\"' or s[0]=='\'') and s[0]==s[-1]:
		s=s[1:-1]
	return(s)

def schema_init(meta_file):
	fmetadata=open(meta_file, "r").readlines()
	i=0
	while i < len(fmetadata):
		if fmetadata[i]=="<begin_table>\n":
			table_name=fmetadata[i+1][:-1]
			table_rows=[]
			i+=2
			while fmetadata[i]!="<end_table>\n":
				table_rows.append(fmetadata[i][:-1])
				i+=1
		i+=1
		schema[table_name]=table_rows

def database_init():
	global schema
	tables=[]
	for i in schema:
		table_name=i+".csv"
		tables.append(table_name)
		rows=[]
		f_table=open(table_name, "r").readlines()
		for j in f_table:
			r=j.strip().split(",")
			for k in range(len(r)):
				r[k]=clean(r[k])
				r[k]=int(r[k])
			rows.append(r)
		database[i.upper()]=rows

	s={}
	for i in schema:
		t=[]
		for j in schema[i]:
			t.append(j.upper())
		s[i.upper()]=t
	schema=s

def cartesian(table1, table2, temp_schema):
	if len(temp_schema)==0:
		temp_schema=schema[table1][:]
		m=database[table1]
	else:
		m=table1
	temp_schema.extend(schema[table2])
	temp_table=[]
	for i in m:
		for j in database[table2]:
			l=i[:]
			l.extend(j)
			temp_table.append(l)
	return(temp_table, temp_schema)

def select_aggr_cols(cols):
	temp_cols=[]
	aggr_cols=[]
	aggr_func=[]
	for i in cols:
		i=i.strip()
		i=i.upper()
		if "MIN(" in i:
			i=i.split("MIN(")
			i=i[1][:-1]
			i=i.strip()
			aggr_cols.append(i)
			aggr_func.append("MIN")
		elif "MAX(" in i:
			i=i.split("MAX(")
			i=i[1][:-1]
			i=i.strip()
			aggr_cols.append(i)
			aggr_func.append("MAX")
		elif "SUM(" in i:
			i=i.split("SUM(")
			i=i[1][:-1]
			i=i.strip()
			aggr_cols.append(i)
			aggr_func.append("SUM")
		elif "AVG(" in i:
			i=i.split("AVG(")
			i=i[1][:-1]
			i=i.strip()
			aggr_cols.append(i)
			aggr_func.append("AVG")
		elif "COUNT(" in i:
			i=i.split("COUNT(")
			i=i[1][:-1]
			i=i.strip()
			aggr_cols.append(i)
			aggr_func.append("COUNT")
		else:
			temp_cols.append(i)
	return(temp_cols, aggr_cols, aggr_func)

def handle_where(lhs, rhs, op, s, temp_table, temp_schema):
	res=set()
	if s==0 or s==1:
		ind_1=temp_schema.index(lhs)
		for i in range(len(temp_table)):
			if op==">=" and temp_table[i][ind_1]>=rhs:
				res.add(i)
			elif op=="<=" and temp_table[i][ind_1]<=rhs:
				res.add(i)
			elif op==">" and temp_table[i][ind_1]>rhs:
				res.add(i)
			elif op=="<" and temp_table[i][ind_1]<rhs:
				res.add(i)
			elif op=="=" and temp_table[i][ind_1]==rhs:
				res.add(i)
	else:
		ind_1=temp_schema.index(lhs)
		ind_2=temp_schema.index(rhs)
		for i in range(len(temp_table)):
			if op==">=" and temp_table[i][ind_1]>=temp_table[i][ind_2]:
				res.add(i)
			elif op=="<=" and temp_table[i][ind_1]<=temp_table[i][ind_2]:
				res.add(i)
			elif op==">" and temp_table[i][ind_1]>temp_table[i][ind_2]:
				res.add(i)
			elif op=="<" and temp_table[i][ind_1]<temp_table[i][ind_2]:
				res.add(i)
			elif op=="=" and temp_table[i][ind_1]==temp_table[i][ind_2]:
				res.add(i)
	return(res)

def validate(select_cols, from_tables, where_conditions, groupby_conditions, orderby_conditions, where_flag, is_distinct):
	res_set=set()
	#Step 1 resolving FROM
	c=0

	for i in from_tables:
		if i in schema:
			c+=1

	if c!=len(from_tables):
		print("Table not found in the database!")
		return

	temp_table={}
	temp_schema={}

	if len(from_tables)==1:
		temp_table["result"]=database[from_tables[0]][:]
		temp_schema["result"]=schema[from_tables[0]]

	elif len(from_tables)>1:
		temp_table["result"], temp_schema["result"]=cartesian(from_tables[0], from_tables[1], [])
		if len(from_tables)>2:
			for i in range(2, len(from_tables)):
				temp_table["result"], temp_schema["result"]=cartesian(temp_table["result"], from_tables[i], temp_schema["result"])
	
	else:
		if len(from_tables)==0:
			print("No FROM component in the query!")
			exit(0)

	#Step 2 resolving WHERE if present
	if len(where_flag)>0:
		lhs=""
		rhs=""
		op=""
		t=[]
		c=0
		for i in where_conditions:
			if ">=" in i:
				x=i.split(">=")
				op=">="
			elif "<=" in i:
				x=i.split("<=")
				op="<="
			elif ">" in i:
				x=i.split(">")
				op=">"
			elif "<" in i:
				x=i.split("<")
				op="<"
			elif "=" in i:
				x=i.split("=")
				op="="
			else:
				print("Error in where condition!")
				exit(0)
			lhs=x[0].strip().upper()
			rhs=x[1].strip().upper()
			
			#Refine the temp table
			s=0
			if lhs in temp_schema["result"] or rhs in temp_schema["result"]:
				if lhs in temp_schema["result"] and rhs in temp_schema["result"]:
					s=2
				elif lhs in temp_schema["result"]:
					s=0
					rhs=int(rhs)
				else:
					s=1
					lhs, rhs=rhs, lhs
					rhs=int(rhs)
					if op==">":
						op="<"
					elif op=="<":
						op=">"
					elif op==">=":
						op="<="
					elif op=="<=":
						op=">="
			else:
				print("Invalid where condition!")
				exit(0)
			if where_flag=="WHERE":
				res_set=handle_where(lhs, rhs, op, s, temp_table["result"], temp_schema["result"])
			elif where_flag=="AND":
				if c==0:
					res=handle_where(lhs, rhs, op, s, temp_table["result"], temp_schema["result"])
					c=1
				elif c==1:
					for i in res_set:
						t.append(temp_table["result"][i])
					temp_table["result"]=t[:]
					res=handle_where(lhs, rhs, op, s, t, temp_schema["result"])
				res_set=set(list(res))
			elif where_flag=="OR":
				res=handle_where(lhs, rhs, op, s, temp_table["result"], temp_schema["result"])
				res_set.update(list(res))

		if len(res_set)>0:
			t=[]
			for i in res_set:
				t.append(temp_table["result"][i])
			temp_table["result"]=t[:]

	#Step 3 resolving GROUP BY if present
	if len(groupby_conditions)>0:
		#To check columns and validity of GROUP BY
		temp_col, aggr_cols, aggr_func=select_aggr_cols(select_cols)
		rem_flag=0
		grpby_col=groupby_conditions[0].upper()

		if len(temp_col)==0:
			temp_col=groupby_conditions[:]
			rem_flag=1

		if len(temp_col)!=1 or grpby_col!=temp_col[0]:
			print("Not correct query for GROUP BY operation!")
			exit(0)
		
		if temp_col[0] not in temp_schema["result"]:
			print("Not correct query!")
			exit(0)

		for i in aggr_cols:
			if i not in temp_schema["result"]:
				if i=="*":
					continue
				print("Not correct query")
				exit(0)

		grp={}
		grp_idx=temp_schema["result"].index(grpby_col)
		grp_i=[]
		grp_t=[]
		temp_col.extend(aggr_cols)
		grp_s=temp_col[:]
		grp_i.append(grp_idx)

		for i in aggr_cols:
			if i=="*" and aggr_func[aggr_cols.index(i)]=="COUNT":
				grp_i.append(grp_idx)
				continue
			elif i=="*" and aggr_func[aggr_cols.index(i)]!="COUNT":
				print("Error in query")
				exit(0)
			else:
				grp_i.append(temp_schema["result"].index(i))

		for i in temp_table["result"]:
			t=[]
			for j in grp_i:
				t.append(i[j])
			grp_t.append(t)

		grp_t.sort()
		count=1
		grp[grp_t[0][0]]=grp_t[0][:]
		prev=grp_t[0][0]

		for i in range(1, len(grp_t)):
			el=grp_t[i][0]
			if prev==el:
				old=grp[prev][:]
				new=grp_t[i][:]
				count+=1
				for j in range(1, len(new)):
					fn=aggr_func[j-1]
					if fn=="MAX":
						new[j]=max(old[j], new[j])
					elif fn=="MIN":
						new[j]=min(old[j], new[j])
					elif fn=="AVG" or fn=="SUM":
						new[j]=new[j]+old[j]
					elif fn=="COUNT":
						new[j]=count
				grp[prev]=new
			else:
				old=grp[prev][:]
				for j in range(1, len(old)):
					fn=aggr_func[j-1]
					if fn=="AVG":
						old[j]=old[j]/count
					elif fn=="COUNT":
						old[j]=count
				grp[prev]=old
				grp[grp_t[i][0]]=grp_t[i][:]
				prev=grp_t[i][0]
				count=1
		if count==1:
			cur=grp[prev][:]
			for j in range(1, len(cur)):
				fn=aggr_func[j-1]
				if fn=="COUNT":
					cur[j]=1
			grp[prev]=cur
		else:
			cur=grp[prev][:]
			for j in range(1, len(cur)):
				fn=aggr_func[j-1]
				if fn=="COUNT":
					cur[j]=count
				elif fn=="AVG":
					cur[j]=cur[j]/count
			grp[prev]=cur

		t=[]
		for i in grp:
			if rem_flag==0:
				t.append(grp[i])
			else:
				t.append(grp[i][1:])

		if len(orderby_conditions)>0 and orderby_conditions[-1]=="DESC":
			t.reverse()

		temp_table["result"]=t[:]
		if rem_flag==0:
			temp_schema["result"]=[grp_s[0]]
		else:
			temp_schema["result"]=[]
		
		for i in range(1, len(grp_s)):
			x=aggr_func[i-1]
			if x=="COUNT":
				x="COUNT("+grp_s[i]+")"
			elif x=="MAX":
				x="MAX("+grp_s[i]+")"
			elif x=="MIN":
				x="MIN("+grp_s[i]+")"
			elif x=="SUM":
				x="SUM("+grp_s[i]+")"
			elif x=="AVG":
				x="AVG("+grp_s[i]+")"
			temp_schema["result"].append(x)
		
		if is_distinct:
			t=[]
			for i in temp_table["result"]:
				if i not in t:
					t.append(i)
			temp_table["result"]=t[:]
		return(temp_table, temp_schema)

	#STEP 4 handle SELECT and after that DISTINCT and ORDER BY
	else:
		temp_col, aggr_cols, aggr_func=select_aggr_cols(select_cols)
		#SELECT on normal column headings
		if len(aggr_cols)==0 and len(temp_col)>0:
			if len(temp_col)==1 and temp_col[0]=="*":
				pass
			else:
				idx=[]
				t1=[]
				try:
					for i in temp_col:
						idx.append(temp_schema["result"].index(i))
				except:
					print("Column not present in table!")
					exit(0)
				for i in temp_table["result"]:
					t2=[]
					for j in idx:
						t2.append(i[j])
					t1.append(t2)
				temp_table["result"]=t1[:]
				temp_schema["result"]=temp_col[:]

		#SELECT on aggregate function headings
		if len(temp_col)==0 and len(aggr_cols)>0:
			idx=[]
			for i in aggr_cols:
				if i=="*" and aggr_func[aggr_cols.index(i)]=="COUNT":
					idx.append(0)
				elif i=="*" and aggr_func[aggr_cols.index(i)]!="COUNT":
					print("Improper usage of * operator!")
					exit(0)
				else:
					idx.append(temp_schema["result"].index(i))
			t1=[]
			for i in temp_table["result"]:
				t2=[]
				for j in idx:
					t2.append(i[j])
				t1.append(t2)
			temp_table["result"]=t1[:]
			temp_schema["result"]=select_cols[:]
			old=[]
			cur=[]
			count=0
			for i in range(len(temp_table["result"])):
				if i==0:
					count=1
					cur=temp_table["result"][0]
					for j in range(len(cur)):
						fn=aggr_func[j]
						if fn=="COUNT":
							cur[j]=1
					old=cur[:]
				else:
					cur=temp_table["result"][i]
					count+=1
					for j in range(len(cur)):
						fn=aggr_func[j]
						if fn=="COUNT":
							cur[j]=old[j]+1
						elif fn=="MAX":
							cur[j]=max(cur[j], old[j])
						elif fn=="MIN":
							cur[j]=min(cur[j], old[j])
						elif fn=="AVG" or fn=="SUM":
							cur[j]+=old[j]
					old=cur[:]
			
			for j in range(len(cur)):
				fn=aggr_func[j]
				if fn=="AVG":
					cur[j]=cur[j]/count

			temp_table["result"]=[cur[:]]
			return(temp_table, temp_schema)

		elif len(temp_col)>0 and len(aggr_cols)>0:
			print("Input query invalid!")
			exit(0)

		#STEP 5 handling DISTINCT
		if is_distinct:
			t=[]
			for i in temp_table["result"]:
				if i not in t:
					t.append(i)
			temp_table["result"]=t[:]

		#STEP 6 handling ORDER BY
		if len(orderby_conditions)>0:
			col=orderby_conditions[0]
			flag=0
			if len(orderby_conditions)>1:
				if orderby_conditions[1]=="DESC":
					flag=1
				else:
					flag=0
			idx=temp_schema["result"].index(col)
			l=sorted(temp_table["result"], key=lambda x: (x[idx]))
			if flag:
				l.reverse()
			temp_table["result"]=l[:]
	return(temp_table, temp_schema)

def process_query(query):
	global schema
	try:
		mode=""
		where_flag=""
		select_cols=[]
		from_tables=[]
		where_conditions=[]
		orderby_conditions=[]
		groupby_conditions=[]
		query=query.split("\n")
		for i in query:
			j=i.split()
			if len(j)>=2:
				if j[0] in ["SELECT", "FROM", "WHERE", "ORDER", "GROUP"]:
					mode=j[0]
			if mode=="SELECT":
				if j[0]=="SELECT":
					for part in j:
						if str(part)=="DISTINCT":
							is_distinct=True
							break
						else:
							is_distinct=False
					if is_distinct:
						temp=j[2:]
					else:
						temp=j[1:]
					temp=" ".join(temp)
					temp=temp.strip(",")
					select_cols.append(clean(temp).upper())
				else:
					temp=j[:]
					temp=" ".join(temp)
					temp=temp.strip(",")
					select_cols.append(clean(temp).upper())
			elif mode=="FROM":
				if j[0]=="FROM":
					temp=j[1:]
					temp=" ".join(temp)
					temp=temp.strip(",")
					from_tables.append(clean(temp).upper())
				else:
					temp=j[:]
					temp=" ".join(temp)
					temp=temp.strip(",")
					from_tables.append(clean(temp).upper())
			elif mode=="WHERE":
				if j[0] in ["WHERE", "AND", "OR"]:
					if j[0]=="AND":
						where_flag="AND"
					elif j[0]=="OR":
						where_flag="OR"
					else:
						where_flag="WHERE"
					temp=j[1:]
					temp=" ".join(temp)
					where_conditions.append(temp)
			elif mode=="ORDER":
				if j[0]=="ORDER" and j[1]=="BY":
					temp=j[2:]
					orderby_conditions=temp
			elif mode=="GROUP":
				if j[0]=="GROUP" and j[1]=="BY":
					temp=j[2:]
					groupby_conditions=temp
			else:
				print("SQL Query is not correct!")
				exit(0)

		temp_table, temp_schema=validate(select_cols, from_tables, where_conditions, groupby_conditions, orderby_conditions, where_flag, is_distinct)
		heading=[]
		for k in temp_schema["result"]:
			if "COUNT(" in k or "MAX(" in k or "MIN(" in k or "AVG(" in k or "SUM(" in k:
				heading.append(k.lower())
			else:
				for j in schema:
					if k in schema[j]:
						x=j.lower()+"."+k.lower()
						heading.append(x)
						break
		print(",".join(heading))
		for k in temp_table["result"]:
			x=""
			for n in k:
				x+=","+str(n)
			print(x[1:])
	except:
		print("Error in handling the query.")
		exit(0)

if __name__ == "__main__":
	if len(sys.argv)==2:
		metadata_filename="metadata.txt"
		schema_init(metadata_filename)
		database_init()
		input_query=sys.argv[1].strip()
		if input_query[-1]==";":
			input_query=input_query[:-1]
			query=sqlparse.format(input_query,reindent=True,keyword_case='upper')
			process_query(query)
		else:
			print("Query not ending with ;")
	else:
		print("Invalid arguments!")