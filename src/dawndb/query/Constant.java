package dawndb.query;

/**
 * The class that denotes values stored in the database.
 * @author Edward Sciore
 */
public class Constant implements Comparable<Constant> {
   private Integer ival = null;
   private String  sval = null;
   
   public Constant(Integer ival) {
      this.ival = ival;
   }
   
   public Constant(String sval) {
      this.sval = sval;
   }
   
   public int asInt() {
      return ival;
   }
   
   public String asString() {
      return sval;
   }
   
   @Override
   public boolean equals(Object obj) {
      Constant c = (Constant) obj;
      return (ival != null) ? ival.equals(c.ival) : sval.equals(c.sval);
   }
   
   @Override
   public int compareTo(Constant c) {
      return (ival != null) ? ival.compareTo(c.ival) : sval.compareTo(c.sval);
   }
   
   @Override
   public int hashCode() {
      return (ival != null) ? ival.hashCode() : sval.hashCode();
   }
   
   @Override
   public String toString() {
      return (ival != null) ? ival.toString() : sval.toString();
   }   
}
