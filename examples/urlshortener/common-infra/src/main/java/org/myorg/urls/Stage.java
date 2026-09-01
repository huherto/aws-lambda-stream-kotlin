package org.myorg.urls;

public enum Stage {
    LOCAL("local"),
    DEV("dev"),
    QA("qa"),
    PROD("prod");

    private final String id;

    Stage(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return id;
    }

    public static Stage parse(String value) {
        for (Stage stage : Stage.values()) {
            if (stage.id.equalsIgnoreCase(value)) {
                return stage;
            }
        }
        throw new IllegalArgumentException("Unknown stage: " + value);
    }
}
