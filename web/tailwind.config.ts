import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        bg: {
          base: "#F7F7F7",
          surface: "#FFFFFF",
          elevated: "#F1F1F1",
          subtle: "#E7E7E7",
        },
        line: {
          DEFAULT: "#D4D4D4",
          muted: "#E5E5E5",
        },
        ink: {
          DEFAULT: "#111111",
          muted: "#525252",
          dim: "#8A8A8A",
        },
        accent: {
          teal: "#111111",
          green: "#16A34A",
          amber: "#737373",
          red: "#DC2626",
          gray: "#737373",
          blue: "#111111",
        },
      },
      fontFamily: {
        sans: [
          "Inter",
          "ui-sans-serif",
          "system-ui",
          "-apple-system",
          "Segoe UI",
          "Roboto",
          "Helvetica Neue",
          "Arial",
          "sans-serif",
        ],
        mono: [
          "JetBrains Mono",
          "ui-monospace",
          "SFMono-Regular",
          "Menlo",
          "Monaco",
          "Consolas",
          "monospace",
        ],
      },
      fontSize: {
        "2xs": ["0.6875rem", { lineHeight: "1rem" }],
      },
      boxShadow: {
        card: "0 1px 0 0 rgba(0,0,0,0.04), 0 12px 40px rgba(0,0,0,0.04)",
      },
      borderRadius: {
        xl: "0.5rem",
        "2xl": "0.5rem",
      },
    },
  },
  plugins: [],
} satisfies Config;
