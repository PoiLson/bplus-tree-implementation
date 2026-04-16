bplus:
	@echo " Compile bplus_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bp_main.c ./src/record.c ./src/bp_file.c ./src/bp_datanode.c ./src/bp_indexnode.c -lbf -o ./build/bplus_main -O2

run:
	rm -f data.db
	./build/bplus_main

test:
	@echo " Compile test_main ...";
	rm -f data.db
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/test_main.c ./src/record.c ./src/bp_file.c ./src/bp_datanode.c ./src/bp_indexnode.c -lbf -o ./build/test_main -O2
	./build/test_main 10000