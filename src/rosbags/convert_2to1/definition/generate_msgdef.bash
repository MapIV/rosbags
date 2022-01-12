#!/bin/bash

#rm -rf msgdef

typename_array=()
check_array() {
    local i
    for i in ${typename_array[@]}; do
        if [[ ${i} = ${1} ]]; then
            return 0
        fi
    done
    return 1
}

while read line
do
	#line='velodyne_msgs/VelodynePacket'
	mkdir -p msgdef/$line
	echo $line
	rosmsg show -r $line > msgdef/$line/msgdef.txt
	rosmsg show $line > msgdef/$line/types.txt
	typename_array=()
	while read types
	do
		if [[ "$types" =~ "/" ]]; then
			array=( $types )
			tmp=${array[0]}
			typename=(${tmp//'['/ })
			if ! check_array $typename; then
				echo "  $typename"
				typename_array=(${typename_array[@]} ${typename})
				echo '================================================================================' >> msgdef/$line/msgdef.txt
				echo "MSG: ${typename}" >> msgdef/$line/msgdef.txt
				rosmsg show -r ${typename} >> msgdef/$line/msgdef.txt
			fi
		fi
	done < msgdef/$line/types.txt

	# Generate md5sum
	rosmsg md5 $line > msgdef/$line/md5sum.txt

	#break
done < msg_list.txt
