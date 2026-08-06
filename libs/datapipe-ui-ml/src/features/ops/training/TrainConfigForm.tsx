import React from "react";
import { Form, Input, InputNumber, Select, Switch } from "antd";
import { TrainConfigJsonEditor } from "./TrainConfigJsonEditor";

type SchemaProperty = {
    type?: string;
    title?: string;
    description?: string;
    enum?: (string | number)[];
    minimum?: number;
    maximum?: number;
    exclusiveMinimum?: number;
    default?: unknown;
    nullable?: boolean;
    ui_group?: string;
    ui_order?: number;
};

type Props = {
    /** JSON Schema object (the `schema` field of the config-schema response). */
    schema?: Record<string, unknown> | null;
    value: Record<string, unknown>;
    onChange: (params: Record<string, unknown>) => void;
    onValidityChange?: (valid: boolean) => void;
    disabled?: boolean;
};

function readProperties(schema?: Record<string, unknown> | null): Record<string, SchemaProperty> {
    if (!schema || typeof schema !== "object") return {};
    const properties = (schema as { properties?: unknown }).properties;
    if (!properties || typeof properties !== "object") return {};
    return properties as Record<string, SchemaProperty>;
}

type FieldEntry = { key: string; prop: SchemaProperty };

function groupFields(properties: Record<string, SchemaProperty>): { group: string; fields: FieldEntry[] }[] {
    const byGroup = new Map<string, FieldEntry[]>();
    Object.entries(properties).forEach(([key, prop]) => {
        const group = prop.ui_group ?? "general";
        if (!byGroup.has(group)) byGroup.set(group, []);
        byGroup.get(group)!.push({ key, prop });
    });
    return Array.from(byGroup.entries()).map(([group, fields]) => ({
        group,
        fields: [...fields].sort(
            (a, b) => (a.prop.ui_order ?? 999) - (b.prop.ui_order ?? 999),
        ),
    }));
}

function labelFor(key: string, prop: SchemaProperty): string {
    return prop.title ?? key;
}

/**
 * Schema-driven editor for train-config params. Falls back to a raw JSON editor
 * when no schema is available, and offers an "Advanced (JSON)" toggle otherwise.
 */
export function TrainConfigForm({ schema, value, onChange, onValidityChange, disabled }: Props) {
    const properties = React.useMemo(() => readProperties(schema), [schema]);
    const groups = React.useMemo(() => groupFields(properties), [properties]);
    const hasSchema = groups.length > 0;
    const [jsonMode, setJsonMode] = React.useState(false);

    React.useEffect(() => {
        // Schema loads async after the drawer opens; prefer the form when it arrives.
        setJsonMode(!hasSchema);
    }, [hasSchema]);

    const setField = (key: string, next: unknown) => {
        const updated = { ...value };
        if (next === undefined || next === "") {
            delete updated[key];
        } else {
            // Keep explicit null for optional schema fields (shows in Advanced JSON).
            updated[key] = next;
        }
        onChange(updated);
    };

    const renderField = ({ key, prop }: FieldEntry) => {
        const current = value[key];
        const type = prop.type;
        const selectValue =
            current === null || current === undefined ? undefined : (current as string | number);
        let control: React.ReactNode;
        if (Array.isArray(prop.enum)) {
            control = (
                <Select
                    aria-label={key}
                    disabled={disabled}
                    value={selectValue}
                    allowClear
                    onChange={(v) => setField(key, v ?? null)}
                    options={prop.enum.map((option) => ({ label: String(option), value: option }))}
                />
            );
        } else if (type === "boolean") {
            control = (
                <Switch
                    disabled={disabled}
                    checked={Boolean(current)}
                    onChange={(checked) => setField(key, checked)}
                />
            );
        } else if (type === "integer" || type === "number") {
            control = (
                <InputNumber
                    aria-label={key}
                    disabled={disabled}
                    style={{ width: "100%" }}
                    value={typeof current === "number" ? current : undefined}
                    min={prop.minimum}
                    max={prop.maximum}
                    step={type === "integer" ? 1 : "any"}
                    onChange={(v) => setField(key, v ?? (prop.nullable ? null : undefined))}
                />
            );
        } else {
            control = (
                <Input
                    aria-label={key}
                    disabled={disabled}
                    value={current == null ? "" : String(current)}
                    onChange={(e) => setField(key, e.target.value)}
                />
            );
        }
        return (
            <Form.Item
                key={key}
                label={labelFor(key, prop)}
                help={prop.description}
                style={{ marginBottom: 12 }}
            >
                {control}
            </Form.Item>
        );
    };

    if (jsonMode) {
        return (
            <div>
                {hasSchema ? (
                    <div style={{ marginBottom: 8 }}>
                        <Switch
                            size="small"
                            checked
                            disabled={disabled}
                            onChange={() => setJsonMode(false)}
                        />{" "}
                        Advanced (JSON)
                    </div>
                ) : null}
                <TrainConfigJsonEditor
                    value={value}
                    onChange={onChange}
                    onValidityChange={onValidityChange}
                    disabled={disabled}
                />
            </div>
        );
    }

    return (
        <Form layout="vertical" className="te-config-form">
            <div style={{ marginBottom: 8 }}>
                <Switch size="small" checked={false} disabled={disabled} onChange={() => setJsonMode(true)} />{" "}
                Advanced (JSON)
            </div>
            {groups.map(({ group, fields }) => (
                <div key={group} className="te-form-group">
                    {groups.length > 1 ? (
                        <div className="te-form-group-title">
                            {group.charAt(0).toUpperCase() + group.slice(1)}
                        </div>
                    ) : null}
                    {fields.map(renderField)}
                </div>
            ))}
        </Form>
    );
}
