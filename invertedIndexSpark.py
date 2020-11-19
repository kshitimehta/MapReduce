

import io
if __name__ == "__main__":    
    print("Running Inverted Index Test Application on Python.... ")
    hashmap = {}
    print("Reading the input file...")
    file = open("..\\inputfile3.txt","r")
    for row in file:
        # striping the trailing spaces
        row = row.rstrip()
        # splitting the key and value by tab
        content = row.split("\t")
        doc_id = content[0]
        words = content[1:]
        # updating the hashmap with keys as words and values as documents
        for word in words[0].split(","):
            list_docs=[]
            if word in hashmap:
                list_docs = hashmap.get(word)
            list_docs = list(set(list_docs))
            list_docs.append(doc_id)
            hashmap.update({word:list(set(list_docs))})
    print("Writing data to spark output file...")
    with io.open("..\\outputspark_index.txt",'w',encoding="utf-8") as f:
      for k,v in hashmap.items():
          f.write(str(k)+"\t"+str(v))
          f.write("\n")
    print("################################## Inverted Index Test Completed. ##################################")
   

