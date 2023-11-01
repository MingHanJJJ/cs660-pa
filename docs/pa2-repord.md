# Programming Assignment 2 report

## Design of BtreeFile:
#### 1. Design of `BtreeFile::splitInternalPage`:

First, we use `getEmptyPage` to create a new Internal page. Then, we iterate through the internal page. For the second 
half of the page, we delete it from page and push it to  a `BtreeEntry` vector to store all the 
`BtreeEntry` that we are going to insert to the new internal page. 

Secondly, we iterate the vector of `BtreeEntry`, and insert it to the new internal page we just created. However, 
we need to push the first entry up to the parent. I used `getParentWithEmptySlots` to get parent page and split parent 
internal page if needed. Then, insert entry to parent page and `updateParentPointers`.

#### 2. Design of `BTreeFile::splitLeafPage`:
First, use `getEmptyPage` to create new leaf page. Then, update left and right sibling pointers from both two pages. I 
iterate through the page. After deleting the second half part of the page and inserting into a vector, I then traverse 
the vector and insert tuples to new leaf page.

Second, we create a `BtreeEntry` base on the key value of first tuple in the new leaf page. Insert the entry to parent 
internal page and `updateParentPointers`.

#### 3. Design of `BTreeFile::findLeafPage`:
I use recurrence to implement this function. If the input page type is a leaf page return the page pointer by 
`BtreeFile::getPage`. However, if the input page type is internal page, we would traverse the internal page and compare 
the key value to the input field. If the key is greater or equal to field value, call `findLeafPage` again to go down to the 
left child of internal page. Moreover, if no key is grater than the field value, we would go down to the right child of last 
element in the internal page.

In addition, if the field value is NULL, we would just simply return the left child form `findLeafPage`.