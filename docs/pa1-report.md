# PA-1 Report
### Tests
I followed all the instruction to build all the class in 'pa1.md', and it passed all the test cases.
### Heapfile
Heapfile only contains filename, page size, and `read` function that would read from the providing file(disk). The `read` 
function would be called by `BufferPool.getPage()` when the corresponding page is not found in the buffer. In `read` function,
it would calculate the offset and only read 1 page size data, and return back to bufferpool. `Bufferpool` would then put 
the page in the buffer (which is a vector in my implementation).
### HeapfileIterator
This class would use the `HeapPageIterator` to iter through the pages in the file. Once it reaches the end of each page. 
It would call `BufferPool.getPage()` to read page from the disk or the buffer. Then, the iterator would start from the 
beginning of new page.
### SeqScan
SeqScan would scan through the all the tuples in the table. Since we already have the `HeapfileIterator`, I use 
`HeapfileIterator` in `SeqScanIterator`. So that we can get ever tuples from the heapfile corresponding to `tableid`.  
### Data files
Since the original `heapfile.dat` is empty, I generated by my self. I copied the matrix from `HeapPageRead_test.cpp`, 
and use `io.cpp` to generate my own `heapfile.dat`. The file contains 3 pages, each pages have same header and same 20 
tuples. Each tuples contain 2 integers. 
### SeqScan test
Since I use my own `heapfile.dat`, i wrote my own test `SeqScanTest.InterateFile2` for the file. The output should be 
3 identical pages which are same as the matrix in `HeapPageRead_test`. 
