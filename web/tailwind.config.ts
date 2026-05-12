import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        bg: {
          base: "#0F172A",
          surface: "#111827",
          elevated: "#1A2233",
          subtle: "#0B1220",
        },
        line: {
          DEFAULT: "#1F2937",
          muted: "#1A2233",
        },
        ink: {
          DEFAULT: "#E5E7EB",
          muted: "#9CA3AF",
          dim: "#6B7280",
        },
        accent: {
          teal: "#22D3EE",
          green: "#10B981",
          amber: "#F59E0B",
          red: "#EF4444",
          gray: "#6B7280",
          blue: "#3B82F6",
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
        card: "0 1px 0 0 rgba(255,255,255,0.02) inset, 0 1px 2px rgba(0,0,0,0.35)",
      },
      borderRadius: {
        xl: "0.75rem",
        "2xl": "1rem",
      },
    },
  },
  plugins: [],
} satisfies Config;
