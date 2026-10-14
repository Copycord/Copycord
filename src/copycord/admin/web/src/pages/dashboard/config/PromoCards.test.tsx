import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { PromoCards } from "./PromoCards";

describe("PromoCards", () => {
  it("gives every card the old hover lift, border, shadow, sweep and icon glow", () => {
    render(<PromoCards />);
    const cards = screen.getAllByRole("link");
    expect(cards).toHaveLength(3);
    for (const card of cards) {
      expect(card).toHaveClass(
        "group",
        "hover:-translate-y-0.5",
        "hover:border-[rgba(214,188,250,0.35)]",
        "hover:[box-shadow:0_12px_28px_rgba(0,0,0,0.32)]",
        "duration-180",
        "hover:after:[transform:translateX(70%)]",
      );
      expect(card.querySelector(".promo-icon")).toHaveClass("group-hover:[transform:translateY(-1px)]");
    }
    expect(cards[0]).toHaveClass("[--icon-glow-rgb:255,213,79]");
    expect(cards[1]).toHaveClass("[--icon-glow-rgb:88,101,242]");
    expect(cards[2]).toHaveClass("[--icon-glow-rgb:236,72,153]");
  });

  it("wobbles the GitHub star, spins it on hover and beats the donate heart on hover", () => {
    render(<PromoCards />);
    const star = document.querySelector(".promo-star .promo-badge svg");
    expect(star).toHaveClass(
      "animate-[cc-star-wobble_2.6s_ease-in-out_infinite]",
      "group-hover:animate-[cc-spin-360_650ms_linear_1_forwards]",
    );
    const heart = document.querySelector(".promo-donate .promo-icon > svg");
    expect(heart).toHaveClass("group-hover:animate-[cc-heartbeat_1.2s_ease-in-out_infinite]");
  });
});
