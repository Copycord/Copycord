// Customize channel / category dialog (#customize-modal, #customize-cat-modal):
// one name input, empty clears the customization.

import { useState } from "react";
import { Modal } from "../../../components/Modal";
import { Button, Field, Input } from "../../../components/ui";

export interface CustomizeModalProps {
  kind: "channel" | "category";
  initial: string;
  saving: boolean;
  onSave: (name: string) => void;
  onClose: () => void;
}

export function CustomizeModal({ kind, initial, saving, onSave, onClose }: CustomizeModalProps) {
  const [name, setName] = useState(initial);
  const channel = kind === "channel";
  const ids = channel ? { input: "customize-name", save: "customize-save" } : { input: "customize-cat-name", save: "customize-cat-save" };
  return (
    <Modal
      open
      onClose={onClose}
      title={channel ? "Customize channel" : "Customize category"}
      footer={
        <Button id={ids.save} variant="ghost" loading={saving} onClick={() => onSave(name)}>
          Save
        </Button>
      }
    >
      <Field
        id={ids.input}
        label={channel ? "Custom channel name" : "Custom category name"}
        help={`Set a custom ${kind} name. Leave empty to use the original.`}
      >
        <Input
          type="text"
          placeholder="Leave empty to use original name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") onSave(name);
          }}
        />
      </Field>
    </Modal>
  );
}
