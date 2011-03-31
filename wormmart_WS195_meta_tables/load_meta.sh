for file in ./*.sql
do 
	echo "$file"
	mysql wormmart_215 < "$file"
done
