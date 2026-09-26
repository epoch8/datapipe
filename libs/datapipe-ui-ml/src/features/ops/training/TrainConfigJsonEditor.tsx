import React from "react";
import Editor from "react-simple-code-editor";
import { highlight, languages } from "prismjs";
import "prismjs/components/prism-json";
import "prismjs/themes/prism.css";

type Props = {
    value: Record<string, unknown>;
    onChange: (params: Record<string, unknown>) => void;
    onValidityChange?: (valid: boolean) => void;
    disabled?: boolean;
};

/**
 * Raw JSON editor for train-config params. Used as a fallback when there is no
 * schema, or as an "advanced" toggle inside {@link TrainConfigForm}.
 *
 * Params are never persisted to localStorage — the editor is fully controlled
 * by the parent.
 */
export function TrainConfigJsonEditor({ value, onChange, onValidityChange, disabled }: Props) {
    const [text, setText] = React.useState<string>(() => JSON.stringify(value ?? {}, null, 2));
    const [error, setError] = React.useState<string | null>(null);
    const lastEmitted = React.useRef<string>(JSON.stringify(value ?? {}));

    // Sync external value changes (e.g. switching experiments) into the text box
    // without clobbering in-progress edits.
    React.useEffect(() => {
        const incoming = JSON.stringify(value ?? {});
        if (incoming !== lastEmitted.current) {
            setText(JSON.stringify(value ?? {}, null, 2));
            setError(null);
            lastEmitted.current = incoming;
        }
    }, [value]);

    const handleChange = (next: string) => {
        setText(next);
        try {
            const parsed = next.trim() ? JSON.parse(next) : {};
            if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
                throw new Error("Config must be a JSON object");
            }
            setError(null);
            onValidityChange?.(true);
            lastEmitted.current = JSON.stringify(parsed);
            onChange(parsed as Record<string, unknown>);
        } catch (e) {
            setError(e instanceof Error ? e.message : "Invalid JSON");
            onValidityChange?.(false);
        }
    };

    return (
        <div className={`te-json-editor${disabled ? " te-json-editor-disabled" : ""}`}>
            <Editor
                aria-label="Config JSON"
                value={text}
                onValueChange={handleChange}
                highlight={(code) => highlight(code, languages.json, "json")}
                disabled={disabled}
                padding={12}
                textareaClassName="te-json-textarea"
                preClassName="te-json-pre"
                className="te-json-code"
                style={{
                    fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace",
                    fontSize: 13,
                    lineHeight: 1.5,
                    minHeight: 180,
                }}
            />
            {error ? <div className="te-json-error">{error}</div> : null}
        </div>
    );
}
