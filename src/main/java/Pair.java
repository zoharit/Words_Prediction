import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;



public class Pair implements WritableComparable<Pair> {
    public  long first;
    public  long second;

    Pair(long first, long second)
    {
        this.first=first;
        this.second=second;
    }
    Pair()
    {
        this.first=-1;
        this.second=-1;
    }
    public int compareTo(Pair o)
    {
        long ret =first - o.first;
        if (ret == 0)
            ret =  (second - o.second);
        int res;
        if(ret>0)
        {
            res=1;
        }
        else  if(ret<0)
        {
            res=-1;
        }
        else{
            res=0;
        }
        return res;
    }

    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeLong(first);
        dataOutput.writeLong(second);
    }

    public void readFields(DataInput dataInput) throws IOException
    {
        this.first=dataInput.readLong();
        this.second=dataInput.readLong();

    }
}
