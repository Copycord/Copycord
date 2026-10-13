// Heroicons outline paths the old scraper.html and scraper.js drew inline.

import type { SVGProps } from "react";

type IconProps = SVGProps<SVGSVGElement> & { size?: number };

function Icon({ size = 14, strokeWidth = 2, children, ...rest }: IconProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={strokeWidth}
      aria-hidden="true"
      {...rest}
    >
      {children}
    </svg>
  );
}

const ROUND = { strokeLinecap: "round", strokeLinejoin: "round" } as const;

export function SearchIcon(p: IconProps) {
  return (
    <Icon size={26} strokeWidth={1.5} {...p}>
      <path {...ROUND} d="m21 21-5.197-5.197m0 0A7.5 7.5 0 1 0 5.196 5.196a7.5 7.5 0 0 0 10.607 10.607Z" />
    </Icon>
  );
}

export function KeyIcon(p: IconProps) {
  return (
    <Icon size={32} strokeWidth={1.5} {...p}>
      <path
        {...ROUND}
        d="M15.75 5.25a3 3 0 0 1 3 3m3 0a6 6 0 0 1-7.029 5.912c-.563-.097-1.159.026-1.563.43L10.5 17.25H8.25v2.25H6v2.25H2.25v-2.818c0-.597.237-1.17.659-1.591l6.499-6.499c.404-.404.527-1 .43-1.563A6 6 0 1 1 21.75 8.25Z"
      />
    </Icon>
  );
}

export function CheckIcon(p: IconProps) {
  return (
    <Icon {...p}>
      <path {...ROUND} d="m4.5 12.75 6 6 9-13.5" />
    </Icon>
  );
}

export function XIcon(p: IconProps) {
  return (
    <Icon {...p}>
      <path {...ROUND} d="M6 18 18 6M6 6l12 12" />
    </Icon>
  );
}

export function RefreshIcon(p: IconProps) {
  return (
    <Icon {...p}>
      <path
        {...ROUND}
        d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182"
      />
    </Icon>
  );
}

export function TrashIcon(p: IconProps) {
  return (
    <Icon {...p}>
      <path
        {...ROUND}
        d="m14.74 9-.346 9m-4.788 0L9.26 9m9.968-3.21c.342.052.682.107 1.022.166m-1.022-.165L18.16 19.673a2.25 2.25 0 0 1-2.244 2.077H8.084a2.25 2.25 0 0 1-2.244-2.077L4.772 5.79m14.456 0a48.108 48.108 0 0 0-3.478-.397m-12 .562c.34-.059.68-.114 1.022-.165m0 0a48.11 48.11 0 0 1 3.478-.397m7.5 0v-.916c0-1.18-.91-2.164-2.09-2.201a51.964 51.964 0 0 0-3.32 0c-1.18.037-2.09 1.022-2.09 2.201v.916m7.5 0a48.667 48.667 0 0 0-7.5 0"
      />
    </Icon>
  );
}

export function QueueIcon(p: IconProps) {
  return (
    <Icon size={16} {...p}>
      <path
        {...ROUND}
        d="M3.75 12h16.5m-16.5 3.75h16.5M3.75 19.5h16.5M5.625 4.5h12.75a1.875 1.875 0 0 1 0 3.75H5.625a1.875 1.875 0 0 1 0-3.75Z"
      />
    </Icon>
  );
}

export function PlayIcon(p: IconProps) {
  return (
    <Icon size={16} {...p}>
      <path
        {...ROUND}
        d="M5.25 5.653c0-.856.917-1.398 1.667-.986l11.54 6.347a1.125 1.125 0 0 1 0 1.972l-11.54 6.347a1.125 1.125 0 0 1-1.667-.986V5.653Z"
      />
    </Icon>
  );
}
