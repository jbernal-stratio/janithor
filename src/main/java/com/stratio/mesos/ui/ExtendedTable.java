package com.stratio.mesos.ui;

import com.googlecode.lanterna.gui2.table.Table;
import com.googlecode.lanterna.input.KeyStroke;
import com.googlecode.lanterna.input.KeyType;

import java.util.HashMap;

/**
 * Created by alonso on 28/11/17.
 */
public class ExtendedTable<V> extends Table<V> {

    public static final String SHOW_RESOURCES_KEY = "r";
    public static final String SELECTED_ROW_HANDLER = "SELECTED_ROW_HANDLER";

    private HashMap<String, Runnable> keymap = new HashMap<>();
    private int lastSelectedRow = -1;

    public ExtendedTable(String... columnLabels) {
        super(columnLabels);
    }

    public void addKeyStrokeHandler(String key, Runnable callback) {
        keymap.put(key, callback);
    }

    public void addSelectedRowHandler(Runnable callback) {
        keymap.put(SELECTED_ROW_HANDLER, callback);
    }

    @Override
    public Result handleKeyStroke(KeyStroke keyStroke) {
        Result result = super.handleKeyStroke(keyStroke);
        // System.err.println(keyStroke.toString());
        if (keyStroke!=null) {
            if (KeyType.ArrowDown.equals(keyStroke.getKeyType()) || KeyType.ArrowUp.equals(keyStroke.getKeyType())) {
                if (keymap.containsKey(SELECTED_ROW_HANDLER) && lastSelectedRow!=getSelectedRow()) {
                    keymap.get(SELECTED_ROW_HANDLER).run();
                }
                lastSelectedRow = getSelectedRow();
            } else if (keyStroke.getCharacter()!=null
                    && SHOW_RESOURCES_KEY.equalsIgnoreCase(keyStroke.getCharacter().toString())) {
                keymap.get(SHOW_RESOURCES_KEY).run();
            }
        }
        return result;
    }

}
