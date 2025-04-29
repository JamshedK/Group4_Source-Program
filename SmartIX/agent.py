# agent.py

import random
import pprint
import time

class Agent:
    """Tabular Q-learning agent with ε-greedy exploration and experience replay."""

    # Maximum episodes and steps per episode for training
    MAX_TRAINING_EPISODES = 50
    MAX_STEPS_PER_EPISODE = 100

    def __init__(self):
        # Per-episode statistics
        self.episode_reward   = {}
        self.episode_duration = {}
        self.episode_mse      = {}

        # RL variables
        self.state      = None
        self.next_state = None
        self.reward     = 0.0
        self.action     = None

        # Hyperparameters
        self.alpha   = 0.01  # learning rate
        self.gamma   = 0.8   # discount factor
        self.epsilon = 0.9   # exploration probability

        # Q-table: maps repr(state) → dict(action → Q-value)
        self.Q = {}

        # Replay memory of (state, action, reward, next_state)
        self.replay_memory = []

    def _ensure_state(self, state):
        """Initialize Q-values to 0.0."""
        key = repr(state)
        if key not in self.Q:
            self.Q[key] = {
                action: 0.0
                for action in self.env.get_available_actions(state)
            }
        return key

    def argmax_a(self, state):
        # Return the action with the highest Q-value in this state.
        key = self._ensure_state(state)
        best_action = None
        max_q = float('-inf')
        for action in self.env.get_available_actions(state):
            q = self.Q[key].get(action, 0.0)
            if q > max_q:
                max_q = q
                best_action = action
        return best_action

    def get_random_action(self, state):
        #Pick a random valid action (for exploration).
        return random.choice(self.env.get_available_actions(state))

    def get_action_epsilon_greedy(self, state):
        """ε-greedy action selection."""
        if random.random() > self.epsilon:
            print("action_type = argmax")
            return self.argmax_a(state)
        else:
            print("action_type = random")
            return self.get_random_action(state)

    def max_a(self, state):
        # Return max_a Q(state, a).
        key = self._ensure_state(state)
        q_vals = [self.Q[key][a] for a in self.env.get_available_actions(state)]
        return max(q_vals) if q_vals else 0.0

    def update(self, state, action, reward, next_state):
        # Update the q-value for the given state-action pair.
        s_key  = self._ensure_state(state)
        ns_key = self._ensure_state(next_state)

        best_next = self.max_a(next_state)
        td_target = reward + self.gamma * best_next
        td_error  = td_target - self.Q[s_key][action]
        self.Q[s_key][action] += self.alpha * td_error

        return td_error**2

    def experience_replay(self):
        # Replay random minibatch of past transitions to reinforce learnin.
        batch_size = min(32, len(self.replay_memory))
        samples = random.sample(self.replay_memory, batch_size)
        for s, a, r, ns in samples:
            self.update(s, a, r, ns)

    def train(self, env):
        """
        Main training loop:
        - For each episode:
          • reset stats, loop MAX_STEPS_PER_EPISODE steps:
            – choose action ε-greedily
            – step env → (next_state, reward)
            – store transition, do one-step update
            – optionally replay past transitions
          • end episode: log stats, decay ε, reset env
        """
        self.env   = env
        self.state = self.env.reset()

        for episode in range(self.MAX_TRAINING_EPISODES):
            # initialize episode stats
            self.episode_reward[episode] = 0.0
            self.episode_mse[episode]    = 0.0
            start_time = time.time()

            print(f"\n=== Episode {episode} ===")
            for step in range(self.MAX_STEPS_PER_EPISODE):
                print(f" step = {step}")

                # select and execute action
                self.action = self.get_action_epsilon_greedy(self.state)
                print(" action =", repr(self.action))
                self.next_state, self.reward = self.env.step(self.action)

                # record transition
                self.replay_memory.append([self.state, self.action, self.reward, self.next_state])

                # update Q-table and accumulate MSE
                td_err_sq = self.update(self.state, self.action, self.reward, self.next_state)
                self.episode_mse[episode] += td_err_sq

                # advance state & stats
                self.state = self.next_state
                self.episode_reward[episode] += self.reward

                # experience replay (after first episode)
                if episode > 0:
                    self.experience_replay()

            # end of episode: log & reset
            duration = time.time() - start_time
            self.episode_duration[episode] = duration
            self.episode_mse[episode] /= self.MAX_STEPS_PER_EPISODE

            print(f"--- Finished Episode {episode} ---")
            print(" Epsilon:", self.epsilon)
            print(" Reward:", self.episode_reward[episode])
            print(" Duration:", duration)

            # let environment handle post-episode logging
            self.env.post_episode(
                episode,
                self.episode_reward[episode],
                duration,
                self.episode_mse[episode]
            )

            # decay exploration rate
            self.epsilon -= self.epsilon * 0.2

            # reset for next episode
            self.state = self.env.reset()
            self.next_state = None
            self.action     = None
            self.reward     = None
