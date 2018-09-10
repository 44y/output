OUTPUT_FILE_NAME="audit_output"



time_now=`date +%s`
go build -o $OUTPUT_FILE_NAME output_main.go
file_time=`stat -c %Y $OUTPUT_FILE_NAME`


if [ $file_time -gt $time_now ]
then
	echo "build ok"
	tar -zcvf "$OUTPUT_FILE_NAME.tar.gz" $OUTPUT_FILE_NAME ./template config.ini install.sh
else
	echo "build error"
fi
