package com.stratio.mesos;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.gui2.*;
import com.googlecode.lanterna.gui2.dialogs.ActionListDialogBuilder;
import com.googlecode.lanterna.gui2.table.Table;
import com.googlecode.lanterna.screen.Screen;
import com.googlecode.lanterna.screen.TerminalScreen;
import com.googlecode.lanterna.terminal.DefaultTerminalFactory;
import com.googlecode.lanterna.terminal.Terminal;
import com.googlecode.lanterna.terminal.swing.SwingTerminalFrame;
import com.stratio.mesos.api.ApiBuilder;
import com.stratio.mesos.api.ExhibitorApi;
import com.stratio.mesos.api.MarathonApi;
import com.stratio.mesos.api.MesosApi;
import com.stratio.mesos.api.framework.MesosFramework;
import com.stratio.mesos.api.framework.MesosResource;
import com.stratio.mesos.api.framework.MesosTask;
import com.stratio.mesos.ui.ExtendedTable;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by alonso on 27/11/17.
 */
public class Interactive {
    public static final String URL = "url";
    public static final String USER = "user";
    public static final String PASS = "pass";
    public static final String TOKEN = "token";
    public static final String PREFIX = "prefix";
    public static final String SECRETS = "secrets-path";

    public static final int FULL_UNINSTALL = -1;
    public static final int TEARDOWN = 1;
    public static final int UNRESERVE = 2;
    public static final int EXHIBITOR = 3;
    public static final int MARATHON = 4;

    private static String masterHost;
    private static String masterPort;
    private static Environment env;

    private static MultiWindowTextGUI gui;
    private static Terminal terminal;
    private static Screen screen;

    private static ObjectMapper jsonParser;

    private static Panel mainPanel;
    private static Panel frameworksPanel;
    private static Panel headerPanel;
    private static Panel pFrameworks;
    private static Panel pResources;
    private static Panel pFilter;

    private static AtomicBoolean appRunning;

    public static void main(String[] args) throws IOException, InterruptedException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        jsonParser = new ObjectMapper();
        jsonParser.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        appRunning = new AtomicBoolean(true);

        try {
            env = mapper.readValue(new File(args[0]), Environment.class);
            System.out.println(ReflectionToStringBuilder.toString(env, ToStringStyle.MULTI_LINE_STYLE));
        } catch (Exception e) {
            System.err.println("Unable to load environment file");
            return;
        }

        TerminalSize size = new TerminalSize(14, 3);
        // Obtain mesos master
        MesosApi mesos = ApiBuilder.build(env.getMasters()[0], MesosApi.class);
        Optional<String> mesosMaster = mesos.findMesosMaster();

        if (mesosMaster.isPresent()) {
            String[] master = mesosMaster.get().split(":");
            masterHost = master[0];
            masterPort = master[1];
        }

        MesosApi finalMesos = mesos;

        // Setup terminal and screen layers
        terminal = new DefaultTerminalFactory().createTerminal();
        screen = new TerminalScreen(terminal);
        screen.startScreen();

        // Create gui and start gui
        gui = new MultiWindowTextGUI(screen, new DefaultWindowManager(), new EmptySpace(TextColor.ANSI.BLUE));
        if (terminal instanceof SwingTerminalFrame) {
            ((SwingTerminalFrame) terminal).setSize(1300, 800);
        }

        // Create windows and panels
        BasicWindow listFrameworksWindow = new BasicWindow();
        listFrameworksWindow.setHints(Arrays.asList(Window.Hint.FULL_SCREEN));
        BasicWindow listTasksWindow = new BasicWindow();
        listTasksWindow.setHints(Arrays.asList(Window.Hint.CENTERED));

        // Initialize panels with tables, radio buttons, etc
        mainPanel = new Panel();
        frameworksPanel = new Panel();
        headerPanel = new Panel();
        pFrameworks = new Panel();
        pResources = new Panel();
        pFilter = new Panel();

        mainPanel.setLayoutManager(new LinearLayout(Direction.VERTICAL));
        frameworksPanel.setLayoutManager(new LinearLayout(Direction.VERTICAL));
        headerPanel.setLayoutManager(new LinearLayout(Direction.HORIZONTAL));
        pFilter.setLayoutManager(new LinearLayout(Direction.HORIZONTAL));
        pFrameworks.setLayoutManager(new GridLayout(1));
        pResources.setLayoutManager(new GridLayout(1));

        // Frameworks table and active/inactive radio button
        ExtendedTable<String> tFrameworks = new ExtendedTable<>("Nº", "Active", "Id", "Name", "Role", "Principal");
        ExtendedTable<String> tResources = new ExtendedTable<>("Resource", "Type", "Amount", "Role", "Principal", "Agent");
        RadioBoxList<String> radioBoxList = new RadioBoxList<>(size);
        radioBoxList.addItem("Active");
        radioBoxList.addItem("Inactive");
        radioBoxList.addListener((i, i1) -> frameworksRefreshListener(mesos, tFrameworks, tResources, radioBoxList));
        radioBoxList.setCheckedItemIndex(0);

        // Add components to panels
        pFrameworks.addComponent(new EmptySpace(new TerminalSize(0, 1)));
        pFrameworks.addComponent(tFrameworks);
        pResources.addComponent(new EmptySpace(new TerminalSize(0, 1)));
        pResources.addComponent(tResources);

        pFilter.addComponent(new EmptySpace(new TerminalSize(0, 1)));
        pFilter.addComponent(radioBoxList);
        pFilter.addComponent(new Button("Refresh list", () -> frameworksRefreshListener(finalMesos, tFrameworks, tResources, radioBoxList)));
        pFilter.addComponent(new EmptySpace(new TerminalSize(0, 1)));

        headerPanel.addComponent(pFilter.withBorder(Borders.singleLine("Filter")));
        frameworksPanel.addComponent(pFrameworks.withBorder(Borders.singleLine("Available frameworks")));
        frameworksPanel.addComponent(pResources.withBorder(Borders.singleLine("Resources for framework")));
        mainPanel.addComponent(headerPanel);
        mainPanel.addComponent(frameworksPanel);

        refreshScreen(terminal.getTerminalSize());

        /////////////////////////////////////////////////////////////////////////////////////////////////////
        // ACTIONS AND LISTENERS
        /////////////////////////////////////////////////////////////////////////////////////////////////////

        // resize panels equally
        terminal.addResizeListener((terminal1, terminalSize) -> refreshScreen(terminalSize));

        // show resources action key
        tFrameworks.addKeyStrokeHandler(ExtendedTable.SHOW_RESOURCES_KEY, () -> {
            try {
                List<String> row = tFrameworks.getTableModel().getRow(tFrameworks.getSelectedRow());
                populateResourcesTable(finalMesos, buildFrameworkPojo(row), tResources);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // action listeners on frameworks list
        tFrameworks.setSelectAction(() -> {
            List<String> data = tFrameworks.getTableModel().getRow(tFrameworks.getSelectedRow());
            MesosFramework selectedFramework = buildFrameworkPojo(data);

            new ActionListDialogBuilder()
                    .setTitle("Framework " + data.get(2))
                    .setDescription("Operations for " + selectedFramework.getName())
                    .addAction("1) Show tasks", () -> {
                        Panel pTasks = new Panel();
                        pTasks.setLayoutManager(new GridLayout(1));
                        Table<String> tTasks = new Table<>("Nº", "Id", "Name", "State", "Slave");
                        Button btReturn = new Button("Return", () -> listTasksWindow.close());

                        int index1 = 0;
                        Optional<List<MesosTask>> tasksFor = finalMesos.findTasksFor(selectedFramework.getId());
                        if (tasksFor.isPresent()) {
                            for (MesosTask mesosTask : tasksFor.get()) {
                                tTasks.getTableModel().addRow(
                                        String.valueOf(++index1),
                                        mesosTask.getId(),
                                        mesosTask.getName(),
                                        mesosTask.getState(),
                                        mesosTask.getSlaveId()
                                );
                                pTasks.addComponent(new EmptySpace(new TerminalSize(0, 1)));
                                pTasks.addComponent(tTasks);
                                pTasks.addComponent(btReturn);

                                listTasksWindow.setComponent(pTasks);
                                gui.addWindow(listTasksWindow);
                            }
                        }
                    })
                    .addAction("2) Uninstall", () -> {
                        ActionListBox actionListBox = showUninstallWindow(selectedFramework);
                        uninstallFramework(selectedFramework, actionListBox, FULL_UNINSTALL);
                    })
                    .addAction("3) Teardown", () -> {
                        ActionListBox actionListBox = showUninstallWindow(selectedFramework);
                        uninstallFramework(selectedFramework, actionListBox, TEARDOWN);
                    })
                    .addAction("4) Unreserve", () -> {
                        ActionListBox actionListBox = showUninstallWindow(selectedFramework);
                        uninstallFramework(selectedFramework, actionListBox, UNRESERVE);
                    })
                    .addAction("5) Exhibitor cleanup", () -> {
                        ActionListBox actionListBox = showUninstallWindow(selectedFramework);
                        uninstallFramework(selectedFramework, actionListBox, EXHIBITOR);
                    })
                    .addAction("6) Marathon destroy", () -> {
                        ActionListBox actionListBox = showUninstallWindow(selectedFramework);
                        uninstallFramework(selectedFramework, actionListBox, MARATHON);
                    })
                    .build()
                    .showDialog(gui);
        });

        listFrameworksWindow.setComponent(mainPanel);
        gui.addWindowAndWait(listFrameworksWindow);

        /////////////////////////////////////////////////////////////////////////////////////////////////////
        // ON DESTROY (RIGHT AFTER WAITING FOR MAIN WINDOW)
        /////////////////////////////////////////////////////////////////////////////////////////////////////
        appRunning.set(false);

    }

    private static void refreshScreen(TerminalSize terminalSize) {
        TerminalSize resourcesSize = new TerminalSize(terminalSize.getColumns(), terminalSize.getRows()/2);
        TerminalSize frameworksSize = new TerminalSize(terminalSize.getColumns(), terminalSize.getRows()/2);

        pResources.setPreferredSize(resourcesSize);
        pFrameworks.setPreferredSize(frameworksSize);
    }

    private static void frameworksRefreshListener(MesosApi mesos, ExtendedTable<String> tFrameworks, ExtendedTable<String> tResources, RadioBoxList<String> radioBoxList) {
        String checkedItems = radioBoxList.getCheckedItem();
        boolean active = "Active".equals(checkedItems)?true:false;
        populateFrameworksTable(mesos, tFrameworks, active);

        clearTable(tResources);
        tResources.getTableModel().addRow("Press R to display resources", "-", "-", "-", "-", "-");

        tFrameworks.setSelectedRow(0);
    }

    private static ActionListBox showUninstallWindow(MesosFramework selectedFramework) {
        BasicWindow uninstallWindow = new BasicWindow();
        uninstallWindow.setHints(Arrays.asList(Window.Hint.CENTERED));

        Panel pUninstall = new Panel();
        pUninstall.setLayoutManager(new GridLayout(1));
        TerminalSize actSize = new TerminalSize(100, 20);
        ActionListBox actionListBox = new ActionListBox(actSize);

        pUninstall.addComponent(new EmptySpace(new TerminalSize(0, 1)));
        pUninstall.addComponent(actionListBox);
        pUninstall.addComponent(new Button("Close", () -> uninstallWindow.close()));

        uninstallWindow.setComponent(pUninstall.withBorder(Borders.singleLine("Uninstalling " + selectedFramework.getName())));
        gui.addWindow(uninstallWindow);
        return actionListBox;
    }

    private static void uninstallFramework(MesosFramework framework, ActionListBox actionListBox, int phase) {
        Runnable taskTeardown = () -> {
            actionListBox.addItem("Looking for secrets...", ()->{});
            String mesosSecret = null;
            mesosSecret = CLI.findMesosSecret(env.getVault().get(URL) + "/userland/passwords/" + framework.getName() + env.getVault().get(SECRETS), env.getVault().get(TOKEN));
            appendStatus(actionListBox, mesosSecret);

            if (mesosSecret==null) {
                actionListBox.addItem("Unable to obtain secret. Aborted", () -> {});
            } else {
                actionListBox.addItem("Obtaining DC/OS token for master...", () -> {});
                String dcosToken = CLI.dcosToken("https://" + masterHost, env.getMarathon().get(USER), env.getMarathon().get(PASS));
                appendStatus(actionListBox, dcosToken);

                String[] credentials = mesosSecret.split(":");
                MesosApi mesosApi = ApiBuilder.build(credentials[0], credentials[1], "http://" + masterHost + ":" + masterPort, MesosApi.class);
                ExhibitorApi exhibitorApi = ApiBuilder.build(credentials[0], credentials[1], env.getExhibitor().get(URL), ExhibitorApi.class);
                MarathonApi marathonApi = ApiBuilder.build(dcosToken, env.getMarathon().get(URL), MarathonApi.class);

                if (phase == -1 || phase == 1) {
                    actionListBox.addItem("Tearing down...", () -> {});
                    boolean teardown = mesosApi.teardown(framework.getId());
                    appendStatus(actionListBox, teardown);
                }

                if (phase == -1 || phase == 2) {
                    actionListBox.addItem("Unreserving resources...", () -> {});
                    List<String> returnCodes = CLI.unreserve(mesosApi, framework.getId(), framework.getRole());
                    appendResources(actionListBox, returnCodes);
                }

                if (phase == -1 || phase == 3) {
                    actionListBox.addItem("Cleaning exhibitor up...", () -> {});
                    boolean exhibitor = exhibitorApi.delete(env.getExhibitor().get(PREFIX) + framework.getName());
                    appendStatus(actionListBox, exhibitor);
                }

                if (phase == -1 || phase == 4) {
                    actionListBox.addItem("Uninstalling marathon app...", () -> {});
                    boolean marathon = marathonApi.destroy(framework.getName());
                    appendStatus(actionListBox, marathon);
                }

                actionListBox.addItem("Done!", () -> {});
            }
        };

        Thread threadUpdate = new Thread(taskTeardown);
        threadUpdate.start();
    }

    private static void appendStatus(ActionListBox actionListBox, boolean status) {
        if (status) {
            actionListBox.addItem("\t> Success", ()->{});
        } else {
            actionListBox.addItem("\t> Failure", ()->{});
        }
    }

    private static void appendStatus(ActionListBox actionListBox, String status) {
        if (status!=null) {
            actionListBox.addItem("\t> " + status, () -> {});
        } else {
            actionListBox.addItem("\t> Failure", () -> {});
        }
    }

    private static void appendResources(ActionListBox actionListBox, List<String> returnCodes) {
        if (returnCodes.size()>0) {
            for (String returnCode : returnCodes) {
                actionListBox.addItem("\t> " + returnCode, () -> {});
            }
        } else {
            actionListBox.addItem("\t> No resources reserved", () -> {});
        }
    }

    private static void populateFrameworksTable(MesosApi mesos, Table<String> tFrameworks, boolean active) {
        clearTable(tFrameworks);

        int index = 0;
        Optional<List<MesosFramework>> frameworks = mesos.findFrameworks(active);
        if (frameworks.isPresent()) {
            for (MesosFramework fwk : frameworks.get()) {
                tFrameworks.getTableModel().addRow(
                        String.valueOf(++index),
                        fwk.getActive().toString(),
                        fwk.getId(),
                        fwk.getName(),
                        fwk.getRole(),
                        fwk.getPrincipal());
            }
        }
    }

    private static void populateResourcesTable(MesosApi mesos, MesosFramework framework, Table<String> tResources) {
        clearTable(tResources);

        Arrays.stream(
                mesos.findSlavesForFramework(framework.getId()).orElse(new String[]{}))
                .forEach(slaveId -> {
                    String[] resourcesFor = mesos.findResourcesFor(framework.getRole(), slaveId);
                    for (String resource : resourcesFor) {
                        try {
                            MesosResource mesosResource = jsonParser.readValue(resource, MesosResource.class);
                            String value = "", principal = "-";
                            if (mesosResource.getScalar()!=null) {
                                value = String.valueOf(mesosResource.getScalar().get("value"));
                            } else if (mesosResource.getRanges()!=null) {
                                value = String.valueOf(mesosResource.getRanges().get("range"));
                            }

                            if (mesosResource.getReservation()!=null) {
                                principal = String.valueOf(mesosResource.getReservation().get("principal"));
                            }

                            tResources.getTableModel().addRow(
                                    mesosResource.getName(),
                                    mesosResource.getType(),
                                    value,
                                    mesosResource.getRole(),
                                    principal,
                                    slaveId);

                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });

        if (tResources.getTableModel().getRowCount()==0) {
            tResources.getTableModel().addRow("No resources found", "-", "-", "-", "-", "-");
        }
    }

    private static void clearTable(Table<String> table) {
        int max = 0;
        if ((max=table.getTableModel().getRowCount())>0) {
            for (int row=0; row<max; row++) {
                table.getTableModel().removeRow(0);
            }
        }
    }

    private static MesosFramework buildFrameworkPojo(List<String> data) {
        return new MesosFramework(Boolean.valueOf(data.get(1)), data.get(2), data.get(3), data.get(4), data.get(5));
    }
}
