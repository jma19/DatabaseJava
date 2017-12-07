package edu.uci.jun.database.rbf;

import edu.uci.jun.database.DatabaseException;
import edu.uci.jun.database.datafiled.DataFiled;
import edu.uci.jun.database.query.QueryPlan;
import edu.uci.jun.database.table.Schema;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An implementation of Iterator that takes in a RecordID iterator provides iteration over Records
 */
public class RecordIterator implements Iterator<Record> {
    private int currentPageNum = 0;
    private RecordBasedFileManager recordBasedFileManager;
    private Page currentPage;
    private int currentSlotNum;
    private String comField;
    private QueryPlan.PredicateOperator predicateOperator;
    private DataFiled comData;
    private List<String> targetFields;
    private Schema schema;

    public RecordIterator(RecordBasedFileManager recordBasedFileManager, Schema schema) {
        this.recordBasedFileManager = recordBasedFileManager;
        this.schema = schema;
        this.currentPage = new Page(recordBasedFileManager.getFileHandle().getFileChannel(), currentPageNum, true);
        this.currentSlotNum = 0;
    }

    /**
     * @param comField
     * @return RecordIterator
     */
    public RecordIterator setComField(String comField) {
        this.comField = comField;
        return this;
    }

    /**
     * @param predicateOperator
     * @return RecordIterator
     */
    public RecordIterator setPredicateOperator(QueryPlan.PredicateOperator predicateOperator) {
        this.predicateOperator = predicateOperator;
        return this;
    }

    /**
     * @param comData
     * @return RecordIterator
     */
    public RecordIterator setComData(DataFiled comData) {
        this.comData = comData;
        return this;
    }

    public RecordIterator setTargetFields(List<String> dataFields) {
        this.targetFields = dataFields;
        return this;
    }

    /**
     * @return
     */
    @Override
    public boolean hasNext() {
        return currentPageNum < recordBasedFileManager.getFileHandle().getCurrentPageNum()
                && currentSlotNum < currentPage.getSlotNum();
    }


    /**
     * @return next record which is not null
     */
    @Override
    public Record next() {
        short slotNum = currentPage.getSlotNum();
        Record record = null;
        while (currentSlotNum < slotNum) {
            RecordID recordID = new RecordID(currentPageNum, currentSlotNum);
            try {
                record = recordBasedFileManager.getRecord(recordID, schema.getFieldTypes());
                record = apply(record);
                currentSlotNum++;
                if (currentSlotNum == slotNum) {
                    currentPageNum++;
                    currentPage = new Page(recordBasedFileManager.getFileHandle().getFileChannel(), currentPageNum, false);
                    currentSlotNum = 0;
                }
                if (record != null) {
                    return record;
                }
            } catch (DatabaseException e) {
                e.printStackTrace();
            }

        }
        return record;
    }

    /**
     * @param record
     * @return the filter record
     */
    private Record apply(Record record) {
        if (predicateOperator == null || predicateOperator.equals(QueryPlan.PredicateOperator.NO_OP)) {
            return record;
        }
        checkNotNull(comData, "comData should not be null");
        List<String> fieldNames = schema.getFieldNames();
        if (comField != null) {
            for (int i = 0; i < fieldNames.size(); i++) {
                if (fieldNames.get(i).equals(comField)) {
                    if (predicateOperator.equals(QueryPlan.PredicateOperator.EQUALS)) {
                        if (record.getValues().get(i).equals(comData)) {
                            return record;
                        }
                    } else if (predicateOperator.equals(QueryPlan.PredicateOperator.GREATER_THAN)) {
                        if (record.getValues().get(i).compareTo(comData) > 1) {
                            return record;
                        }
                    } else if (predicateOperator.equals(QueryPlan.PredicateOperator.GREATER_THAN_EQUALS)) {
                        if (record.getValues().get(i).compareTo(comData) >= 0) {
                            return record;
                        }
                    } else if (predicateOperator.equals(QueryPlan.PredicateOperator.LESS_THAN)) {
                        if (record.getValues().get(i).compareTo(comData) < 0) {
                            return record;
                        }
                    } else if (predicateOperator.equals(QueryPlan.PredicateOperator.LESS_THAN_EQUALS)) {
                        if (record.getValues().get(i).compareTo(comData) <= 0) {
                            return record;
                        }
                    } else if (predicateOperator.equals(QueryPlan.PredicateOperator.NOT_EQUALS)) {
                        if (record.getValues().get(i).compareTo(comData) != 0) {
                            return record;
                        }
                    }
                    return null;
                }
            }
        }
        return null;
    }


    public void remove() {
        throw new UnsupportedOperationException();
    }
}

