import React from "react";
import { Form, Input, Modal, Select } from "antd";
import type { MetricsCandidateCreate, FrozenDatasetRow } from "../../../types/opsMl";

type Props = {
    open: boolean;
    datasets: FrozenDatasetRow[];
    subsets: string[];
    onCancel: () => void;
    onSubmit: (body: MetricsCandidateCreate) => void;
};

export function AddModelCandidateModal({ open, datasets, subsets, onCancel, onSubmit }: Props) {
    const [form] = Form.useForm<MetricsCandidateCreate>();

    React.useEffect(() => {
        if (open) {
            form.resetFields();
            form.setFieldsValue({ subset: subsets.includes("val") ? "val" : subsets[0], model_source: "manual" });
        }
    }, [open, form, subsets]);

    return (
        <Modal
            title="Add model"
            visible={open}
            onCancel={onCancel}
            onOk={() => {
                form.validateFields().then((values) => {
                    onSubmit(values);
                    onCancel();
                });
            }}
            okText="Add"
        >
            <Form form={form} layout="vertical">
                <Form.Item name="model_id" label="Model ID" rules={[{ required: true }]}>
                    <Input placeholder="cat_dog_yolo_smoke" />
                </Form.Item>
                <Form.Item name="model_source" label="Model source">
                    <Select
                        options={[
                            { value: "manual", label: "Manual" },
                            { value: "training_run", label: "Training run" },
                            { value: "artifact", label: "Artifact" },
                            { value: "registry", label: "Registry" },
                        ]}
                    />
                </Form.Item>
                <Form.Item name="dataset_id" label="Dataset" rules={[{ required: true }]}>
                    <Select
                        showSearch
                        options={datasets.map((d) => ({ value: d.dataset_id, label: d.dataset_id }))}
                    />
                </Form.Item>
                <Form.Item name="subset" label="Subset" rules={[{ required: true }]}>
                    <Select options={subsets.map((s) => ({ value: s, label: s }))} />
                </Form.Item>
                <Form.Item name="task_type" label="Task type">
                    <Select
                        allowClear
                        placeholder="auto"
                        options={[
                            { value: "detection", label: "Detection" },
                            { value: "classification", label: "Classification" },
                            { value: "segmentation", label: "Segmentation" },
                            { value: "keypoints", label: "Keypoints" },
                        ]}
                    />
                </Form.Item>
                <Form.Item name="artifact_uri" label="Artifact URI">
                    <Input placeholder="s3://…" />
                </Form.Item>
            </Form>
        </Modal>
    );
}
