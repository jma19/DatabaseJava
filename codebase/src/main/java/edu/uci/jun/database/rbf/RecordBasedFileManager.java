package edu.uci.jun.database.rbf;

import edu.uci.jun.database.DatabaseException;
import edu.uci.jun.database.datafiled.DataFiled;

import java.util.List;

/**
 *
 * Created by junm5 on 11/15/17.
 */
public class RecordBasedFileManager {

    private Page page;
    /**
     * Adds a new record to this table. The record should be added to the first
     * free slot of the first free page if one exists, otherwise a new page should
     * be allocated and the record should be placed in the first slot of that
     * page. Recall that a free slot in the slot bitmap means the bit is set to 0.
     * Make sure to update this.stats, this.freePages, and this.numRecords as
     * necessary.
     *
     * @param values the values of the record being added
     * @return the RecordID of the added record
     * @throws DatabaseException if the values passed in to this method do not
     *         correspond to the schema of this table
     */
    public RecordID addRecord(List<DataFiled> values) throws DatabaseException {

        // TODO: implement me!
        return null;
    }

    /**
     * Deletes the record specified by rid from the table. Make sure to update
     * this.stats, this.freePages, and this.numRecords as necessary.
     *
     * @param rid the RecordID of the record to delete
     * @return the Record referenced by rid that was removed
     * @throws DatabaseException if rid does not correspond to a valid record
     */
    public Record deleteRecord(RecordID rid) throws DatabaseException {
        // TODO: implement me!
        return null;
    }

    /**
     * Retrieves a record from the table.
     *
     * @param rid the RecordID of the record to retrieve
     * @return the Record referenced by rid
     * @throws DatabaseException if rid does not correspond to a valid record
     */
    public Record getRecord(RecordID rid) throws DatabaseException {
        // TODO: implement me!
        return null;
    }

    /**
     * Updates an existing record with new values and returns the old version of the record.
     * Make sure to update this.stats as necessary.
     *
     * @param values the new values of the record
     * @param rid the RecordID of the record to update
     * @return the old version of the record
     * @throws DatabaseException if rid does not correspond to a valid record or
     *         if the values do not correspond to the schema of this table
     */
    public Record updateRecord(List<DataFiled> values, RecordID rid) throws DatabaseException {
        // TODO: implement me!
        return null;
    }
}
