package io.zeebe;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.protocol.record.value.ProcessInstanceRecordValue;

public class DemoExporter implements Exporter {
    Controller controller;

    public void configure(Context context) throws Exception {
        context.getConfiguration();
    }

    public void open(Controller controller) {
        this.controller = controller;
    }

    public void close() {
    }

    public void export(final Record<?> record) {

        try {

            if (isEventSubprocessEnded(record)) {
                System.out.println(" ######  Event Subprocess ended! "
                        + ((ProcessInstanceRecordValue) record.getValue()).getElementId() + " ###### ");

                System.out.println("RecordType: " + record.getRecordType());
                System.out.println("ValueType: " + record.getValueType());
                System.out.println("Intent: " + record.getIntent());
                System.out.println("Value " + record.getValue());
                System.out.println("Value Class " + record.getValue().getClass().getName());
            }

        } finally {
            this.controller.updateLastExportedRecordPosition(record.getPosition());
        }

    }

    private Boolean isEventSubprocessEnded(final Record<?> record) {
        if (!(record.getValue() instanceof ProcessInstanceRecordValue)) {
            return false;
        }
        return record.getRecordType() == RecordType.EVENT &&
                record.getValueType() == ValueType.PROCESS_INSTANCE &&
                record.getIntent() == ProcessInstanceIntent.ELEMENT_COMPLETED &&
                ((ProcessInstanceRecordValue) record.getValue())
                        .getBpmnElementType() == BpmnElementType.EVENT_SUB_PROCESS;
    }
}
