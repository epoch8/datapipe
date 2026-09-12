import React from "react";
import { Alert, Button } from "antd";

type Props = { children: React.ReactNode };
type State = { error: Error | null };

export class ErrorBoundary extends React.Component<Props, State> {
    state: State = { error: null };

    static getDerivedStateFromError(error: Error): State {
        return { error };
    }

    render() {
        if (this.state.error) {
            return (
                <Alert
                    type="error"
                    showIcon
                    message="Something went wrong"
                    description={
                        <>
                            <pre style={{ whiteSpace: "pre-wrap", fontSize: 12 }}>
                                {this.state.error.message}
                            </pre>
                            <Button
                                style={{ marginTop: 8 }}
                                onClick={() => this.setState({ error: null })}
                            >
                                Try again
                            </Button>
                        </>
                    }
                />
            );
        }
        return this.props.children;
    }
}
