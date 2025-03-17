import bodyParser from "body-parser";
import express from "express";
import { BASE_NODE_PORT } from "../config";
import { Value } from "../types";
import { delay } from "../utils";

export async function node(
  nodeId: number, // the ID of the node
  N: number, // total number of nodes in the network
  F: number, // number of faulty nodes in the network
  initialValue: Value, // initial value of the node
  isFaulty: boolean, // true if the node is faulty, false otherwise
  nodesAreReady: () => boolean, // used to know if all nodes are ready to receive requests
  setNodeIsReady: (index: number) => void // this should be called when the node is started and ready to receive requests
) {
  const node = express();
  node.use(express.json());
  node.use(bodyParser.json());

  // Node state
  const state = {
    killed: false,
    x: isFaulty ? null : initialValue,
    decided: isFaulty ? null : false,
    k: isFaulty ? null : 0,
  };

  // To store messages received from other nodes
  const phaseOneMessages: Record<number, { value: Value; from: number }[]> = {};
  const phaseTwoMessages: Record<number, { value: Value; from: number }[]> = {};

  let consensusRunning = false;

  // This route allows retrieving the current status of the node
  node.get("/status", (req, res) => {
    if (isFaulty) {
      res.status(500).send("faulty");
    } else {
      res.status(200).send("live");
    }
  });

  // This route allows the node to receive messages from other nodes
  node.post("/message", (req, res) => {
    if (state.killed || isFaulty) {
      res.status(500).json({ error: "Node is dead or faulty" });
      return;
    }

    const { phase, value, k, from } = req.body;

    if (phase === 1) {
      if (!phaseOneMessages[k]) {
        phaseOneMessages[k] = [];
      }
      phaseOneMessages[k].push({ value, from });
    } else if (phase === 2) {
      if (!phaseTwoMessages[k]) {
        phaseTwoMessages[k] = [];
      }
      phaseTwoMessages[k].push({ value, from });
    }

    res.status(200).json({ success: true });
  });

  // This route is used to start the consensus algorithm
  node.get("/start", async (req, res) => {
    if (isFaulty || state.killed) {
      res.status(500).json({ error: "Node is faulty or killed" });
      return;
    }

    res.status(200).json({ success: true });

    // Start the consensus algorithm if not already running
    if (!consensusRunning) {
      consensusRunning = true;
      startConsensus();
    }
  });

  // This route is used to stop the consensus algorithm
  node.get("/stop", async (req, res) => {
    state.killed = true;
    consensusRunning = false;
    res.status(200).json({ success: true });
  });

  // Get the current state of a node
  node.get("/getState", (req, res) => {
    res.status(200).json(state);
  });

  // Start the consensus algorithm
  async function startConsensus() {
    // Special case for a single node
    if (N === 1 && !isFaulty) {
      state.decided = true;
      return;
    }

    while (!state.decided && !state.killed) {
      await runPhaseOne();
      await runPhaseTwo();
      
      // Move to the next round
      if (!state.decided && !state.killed) {
        state.k = (state.k as number) + 1;
      }
      
      // Add a small delay between rounds to avoid overwhelming the network
      await delay(10);
    }
  }

  // Phase 1 of the Ben-Or algorithm
  async function runPhaseOne() {
    // Skip if the node is killed or has already decided
    if (state.killed || state.decided) return;

    // Broadcast current value to all nodes
    await broadcastMessage(1, state.x as Value, state.k as number);

    // Wait for messages from other nodes
    await waitForMessages(1);

    // Process received messages
    processPhaseOneMessages();
  }

  // Phase 2 of the Ben-Or algorithm
  async function runPhaseTwo() {
    // Skip if the node is killed or has already decided
    if (state.killed || state.decided) return;

    // Broadcast current value to all nodes
    await broadcastMessage(2, state.x as Value, state.k as number);

    // Wait for messages from other nodes
    await waitForMessages(2);

    // Process received messages
    processPhaseTwoMessages();
  }

  // Broadcast a message to all nodes
  async function broadcastMessage(phase: number, value: Value, k: number) {
    const promises = [];
    
    for (let i = 0; i < N; i++) {
      if (i !== nodeId) {
        promises.push(
          fetch(`http://localhost:${BASE_NODE_PORT + i}/message`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              phase,
              value,
              k,
              from: nodeId,
            }),
          }).catch(() => {
            // Ignore errors (node might be faulty)
          })
        );
      }
    }
    
    await Promise.all(promises);
  }

  // Wait for messages from other nodes
  async function waitForMessages(phase: number) {
    const maxWaitTime = 200; // ms
    const startTime = Date.now();
    
    const messages = phase === 1 ? phaseOneMessages : phaseTwoMessages;
    const currentK = state.k as number;
    
    // Wait until we receive enough messages or timeout
    while (
      Date.now() - startTime < maxWaitTime &&
      (!messages[currentK] || messages[currentK].length < N - F - 1)
    ) {
      await delay(10);
      if (state.killed) return;
    }
  }

  // Process messages received during phase 1
  function processPhaseOneMessages() {
    if (state.killed) return;
    
    const currentK = state.k as number;
    if (!phaseOneMessages[currentK]) {
      phaseOneMessages[currentK] = [];
    }

    const messages = phaseOneMessages[currentK];
    const counts: Record<string, number> = { "0": 0, "1": 0 };

    // Count own value
    if (state.x === 0 || state.x === 1) {
      counts[state.x.toString()]++;
    }

    // Count received values
    for (const msg of messages) {
      if (msg.value === 0 || msg.value === 1) {
        counts[msg.value.toString()]++;
      }
    }

    // Check if there's a majority (more than N/2)
    const majority = Math.floor(N / 2) + 1;
    
    if (counts["0"] >= majority) {
      state.x = 0;
    } else if (counts["1"] >= majority) {
      state.x = 1;
    } else {
      state.x = "?";
    }
  }

  // Process messages received during phase 2
  function processPhaseTwoMessages() {
    if (state.killed) return;
    
    const currentK = state.k as number;
    if (!phaseTwoMessages[currentK]) {
      phaseTwoMessages[currentK] = [];
    }

    const messages = phaseTwoMessages[currentK];
    const counts: Record<string, number> = { "0": 0, "1": 0, "?": 0 };

    // Count own value
    if (state.x !== null) {
      counts[state.x.toString()]++;
    }

    // Count received values
    for (const msg of messages) {
      if (msg.value === 0 || msg.value === 1 || msg.value === "?") {
        counts[msg.value.toString()]++;
      }
    }

    // For fault tolerance threshold calculation
    const nonFaultyNodes = N - F;
    
    // Calculate thresholds - adjusted for better fault tolerance
    const decisionThreshold = Math.floor(nonFaultyNodes / 2) + 1;
    const adoptionThreshold = Math.floor(nonFaultyNodes / 3) + 1;

    // Check if we can decide on a value
    if (counts["0"] >= decisionThreshold && state.x === 0) {
      state.decided = true;
    } else if (counts["1"] >= decisionThreshold && state.x === 1) {
      state.decided = true;
    }
    // Check if we can adopt a value for the next round
    else if (counts["0"] >= adoptionThreshold) {
      state.x = 0;
    } else if (counts["1"] >= adoptionThreshold) {
      state.x = 1;
    } else {
      // Choose randomly between 0 and 1
      state.x = Math.random() < 0.5 ? 0 : 1;
    }
  }

  // start the server
  const server = node.listen(BASE_NODE_PORT + nodeId, async () => {
    console.log(
      `Node ${nodeId} is listening on port ${BASE_NODE_PORT + nodeId}`
    );

    // the node is ready
    setNodeIsReady(nodeId);
  });

  return server;
}