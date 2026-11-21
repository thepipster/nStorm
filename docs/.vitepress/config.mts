import { defineConfig } from "vitepress";

const repo = "https://github.com/thepipster/nStorm";

const specNodes = [
    ["Bolt", "bolt"],
    ["Topology", "topology"],
    ["Queue contract", "queue-contract"],
    ["BullMQ queue", "bullmq-queue"],
    ["Simple queue", "simple-queue"],
    ["Async queue", "async-queue"],
    ["Logger", "logger"],
];

export default defineConfig({
    title: "nStorm",
    description:
        "A simple real-time processing system for Node.js, inspired by Storm",
    // Served from https://thepipster.github.io/nStorm/
    base: "/nStorm/",
    cleanUrls: true,
    lastUpdated: true,

    themeConfig: {
        nav: [
            { text: "Guide", link: "/guide/getting-started" },
            { text: "Reference", link: "/reference/nstorm" },
            { text: "Internals", link: "/specs/" },
            { text: "npm", link: "https://www.npmjs.com/package/nstorm" },
        ],

        sidebar: {
            "/guide/": [
                {
                    text: "Introduction",
                    items: [
                        {
                            text: "Getting started",
                            link: "/guide/getting-started",
                        },
                        { text: "Concepts", link: "/guide/concepts" },
                    ],
                },
                {
                    text: "Building a topology",
                    items: [
                        { text: "Writing blocks", link: "/guide/blocks" },
                        {
                            text: "Wiring blocks together",
                            link: "/guide/topologies",
                        },
                        {
                            text: "Failures and replay",
                            link: "/guide/failures",
                        },
                        { text: "Shutting down", link: "/guide/shutdown" },
                    ],
                },
                {
                    text: "Running it",
                    items: [
                        {
                            text: "Queue providers",
                            link: "/guide/queue-providers",
                        },
                        { text: "Scaling out", link: "/guide/scaling" },
                        { text: "Logging", link: "/guide/logging" },
                    ],
                },
                {
                    text: "More",
                    items: [
                        {
                            text: "Writing a queue provider",
                            link: "/guide/custom-providers",
                        },
                        {
                            text: "Upgrading from 4.0",
                            link: "/guide/upgrading",
                        },
                    ],
                },
            ],

            "/reference/": [
                {
                    text: "API reference",
                    items: [
                        { text: "nStorm", link: "/reference/nstorm" },
                        { text: "Options", link: "/reference/options" },
                        { text: "BaseBolt", link: "/reference/basebolt" },
                        { text: "BaseQueue", link: "/reference/basequeue" },
                        { text: "Logger", link: "/reference/logger" },
                    ],
                },
            ],

            "/specs/": internals(),
            "/adr/": internals(),
        },

        search: { provider: "local" },

        socialLinks: [{ icon: "github", link: repo }],

        editLink: {
            pattern: `${repo}/edit/master/docs/:path`,
            text: "Edit this page on GitHub",
        },

        footer: {
            message: "Released under the MIT License.",
            copyright: "Copyright © 2014-present Mike Pritchard",
        },
    },
});

function internals() {
    return [
        {
            text: "Requirements and design",
            items: [
                { text: "How the specs work", link: "/specs/" },
                ...specNodes.map(([text, node]) => ({
                    text,
                    collapsed: true,
                    items: [
                        {
                            text: "Requirements",
                            link: `/specs/intent/${node}/${node}-specs`,
                        },
                        {
                            text: "Design",
                            link: `/specs/intent/${node}/${node}-design`,
                        },
                    ],
                })),
            ],
        },
        {
            text: "Decision records",
            items: [
                { text: "About the decision log", link: "/adr/" },
                {
                    text: "0001 Host owns the process lifecycle",
                    link: "/adr/0001-host-owns-process-lifecycle",
                },
                {
                    text: "0002 SimpleQueue at-most-once in flight",
                    link: "/adr/0002-simple-queue-at-most-once-in-flight",
                },
                {
                    text: "0003 emit() rejects, safe un-awaited",
                    link: "/adr/0003-emit-rejects-but-is-safe-unawaited",
                },
                {
                    text: "0004 Stop drains with a timeout",
                    link: "/adr/0004-stop-drains-with-a-timeout",
                },
            ],
        },
    ];
}
