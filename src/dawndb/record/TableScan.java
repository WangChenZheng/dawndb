package dawndb.record;

import dawndb.file.BlockId;
import dawndb.query.*;
import dawndb.tx.Transaction;

import static java.sql.Types.INTEGER;

/**
 * Provides the abstraction of an arbitrarily large array
 * of records.
 * @author sciore
 */
public class TableScan implements UpdateScan {
   private Transaction tx;
   private Layout layout;
   private RecordPage rp;
   private String filename;
   private int currentslot;

   public TableScan(Transaction tx, String tblname, Layout layout) {
      this.tx = tx;
      this.layout = layout;
      filename = tblname + ".tbl";
      if (tx.size(filename) == 0) {
         moveToNewBlock();
      } else {
         moveToBlock(0);
      }
   }

   // Methods that implement Scan

   @Override
   public void beforeFirst() {
      moveToBlock(0);
   }

   @Override
   public boolean next() {
      currentslot = rp.nextAfter(currentslot);
      while (currentslot < 0) {
         if (atLastBlock()) {
            return false;
         }
         moveToBlock(rp.block().blknum()+1);
         currentslot = rp.nextAfter(currentslot);
      }
      return true;
   }

   @Override
   public int getInt(String fldname) {
      return rp.getInt(currentslot, fldname);
   }

   @Override
   public String getString(String fldname) {
      return rp.getString(currentslot, fldname);
   }

   @Override
   public Constant getVal(String fldname) {
      if (layout.schema().type(fldname) == INTEGER) {
         return new Constant(getInt(fldname));
      } else {
         return new Constant(getString(fldname));
      }
   }

   @Override
   public boolean hasField(String fldname) {
      return layout.schema().hasField(fldname);
   }

   @Override
   public void close() {
      if (rp != null) {
          tx.unpin(rp.block());
      }
   }

   // Methods that implement UpdateScan

   @Override
   public void setInt(String fldname, int val) {
      rp.setInt(currentslot, fldname, val);
   }

   @Override
   public void setString(String fldname, String val) {
      rp.setString(currentslot, fldname, val);
   }

   @Override
   public void setVal(String fldname, Constant val) {
      if (layout.schema().type(fldname) == INTEGER) {
          setInt(fldname, val.asInt());
      } else {
          setString(fldname, val.asString());
      }
   }

   @Override
   public void insert() {
      currentslot = rp.insertAfter(currentslot);
      while (currentslot < 0) {
         if (atLastBlock()) {
             moveToNewBlock();
         } else {
             moveToBlock(rp.block().blknum()+1);
         }
         currentslot = rp.insertAfter(currentslot);
      }
   }

   @Override
   public void delete() {
      rp.delete(currentslot);
   }

   @Override
   public void moveToRid(dawndb.record.RID rid) {
      close();
      BlockId blk = new BlockId(filename, rid.blockNumber());
      rp = new RecordPage(tx, blk, layout);
      currentslot = rid.slot();
   }

   @Override
   public RID getRid() {
      return new RID(rp.block().blknum(), currentslot);
   }

   // Private auxiliary methods

   private void moveToBlock(int blknum) {
      close();
      BlockId blk = new BlockId(filename, blknum);
      rp = new RecordPage(tx, blk, layout);
      currentslot = -1;
   }

   private void moveToNewBlock() {
      close();
      BlockId blk = tx.append(filename);
      rp = new RecordPage(tx, blk, layout);
      rp.format();
      currentslot = -1;
   }

   private boolean atLastBlock() {
      return rp.block().blknum() == tx.size(filename) - 1;
   }
}
