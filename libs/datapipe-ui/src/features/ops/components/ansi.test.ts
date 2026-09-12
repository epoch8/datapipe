import { ansiToHtml, hasAnsi, stripAnsi } from "./ansi";

describe("ansi", () => {
    it("strips CSI color codes", () => {
        const raw = "\u001b[34mINFO\u001b[0m hello";
        expect(stripAnsi(raw)).toBe("INFO hello");
        expect(hasAnsi(raw)).toBe(true);
        expect(hasAnsi("plain")).toBe(false);
    });

    it("renders SGR colors as styled spans and escapes HTML", () => {
        const raw = "\u001b[1;31m<script>\u001b[0m ok";
        const html = ansiToHtml(raw);
        expect(html).toContain('<span style="color:#cd3131;font-weight:700">&lt;script&gt;</span>');
        expect(html).toContain(" ok");
        expect(html).not.toContain("<script>");
    });

    it("supports 256-color and truecolor fg", () => {
        expect(ansiToHtml("\u001b[38;5;196mx\u001b[0m")).toContain("color:");
        expect(ansiToHtml("\u001b[38;2;10;20;30mx\u001b[0m")).toContain(
            "color:rgb(10,20,30)",
        );
    });
});
