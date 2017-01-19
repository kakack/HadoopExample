#!/bin/bash
your_name="Jinx"
echo $your_name
your_name="poppy"
echo ${your_name}

str="my name is \"$your_name\"! \n"
echo $str

greeting="hello, "$your_name" !"
greeting1="hello, ${your_name} !"
echo $greeting $greeting1

echo ${#greeting}
echo ${greeting:2:5}



