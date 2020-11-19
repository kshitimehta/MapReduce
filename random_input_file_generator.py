import numpy as np
import random

def test_case():
    with open("inputfile.txt","w") as f:
        for i in range(10):
            num = random.randint(80,100)
            print(num)
            f.write(str(i))
            for j in range(0,num):
                n = "s"+str(num)
                f.write("\t"+n)
            f.write("\n")
    f.close()


test_case()
    
