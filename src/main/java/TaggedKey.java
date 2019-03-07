import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaggedKey implements WritableComparable<TaggedKey> {
       private String tag;
       private Long key;


       public String getTag(){
           return  this.tag;
       }
       public Long getKey()
       {
           return  this.key;
       }
    TaggedKey(String tag,long key)
    {
        this.tag=tag;
        this.key=key;
    }
       public TaggedKey()
    {
        tag=null;
        key=null;
    }


    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(tag);
        dataOutput.writeLong(key);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.tag=dataInput.readUTF();
        this.key=dataInput.readLong();
    }

    public int compareTo(TaggedKey o) {
            if(o.key!=this.key)
            {
                if(o.key<this.key)
                {return 1;}
                else {
                    return -1;
                }
            }
            else {
                return  this.tag.compareTo(o.tag);
            }


    }
}
