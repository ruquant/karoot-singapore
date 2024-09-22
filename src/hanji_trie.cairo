use starknet::ContractAddress;
use starknet::storage_access::StorageBaseAddress;
use starknet::storage::Map;
use core::traits::Into;
// use core::integer::U256IntoU128;

#[starknet::interface]
trait IHanjiTrie<TContractState> {
    fn addOrder(ref self: TContractState, price: u128, amount: u128, owner: ContractAddress) -> felt252;
    fn removeOrder(ref self: TContractState, price: u128, id: felt252) -> (u128, ContractAddress);
    fn claimExecuted(ref self: TContractState, price: u128, id: felt252, amount: u128) -> (u128, u128);
    fn executeRight(ref self: TContractState, price: u128, amount: u128) -> (u128, ContractAddress);
    fn getOrderInfo(self: @TContractState, price: u128, id: felt252) -> (u128, ContractAddress);
    fn previewExecuteRight(self: @TContractState, price: u128, amount: u128) -> (u128, ContractAddress);
    fn assembleOrderbookFromOrders(self: @TContractState, price: u128, count: u128) -> Array<(u128, ContractAddress)>;
}

#[starknet::contract]
mod HanjiTrie {
    use super::IHanjiTrie;
    use starknet::ContractAddress;
    use starknet::storage_access::StorageBaseAddress;
    use starknet::storage::Map;

    #[storage]
    struct Storage {
        lob_address: ContractAddress,
        best_offer: u64,
        rightmost_map: u64,
        highest_allocated_pointer: u64,
        last_free_pointer: u64,
        leaves: Map::<u64, Leaf>,
        nodes: Map::<u64, Node>,
        trader_id_counter: u64,
        trader_ids: Map<ContractAddress, u64>,
        trader_addresses: Map::<u64, ContractAddress>,
    }

    #[derive(Copy, Drop, Serde, starknet::Store)]
    struct Node {
        left: u64,
        right: u64,
        total_shares: u128,
        total_value: u128,
    }

    #[derive(Copy, Drop, Serde, starknet::Store)]
    struct Leaf {
        trader_id: u64,
        remain_shares: u128,
        initial_shares: u128,
        initial_value: u128,
    }

    #[derive(Copy, Drop, Serde)]
    struct ExecutionResult {
        executed_shares: u128,
        executed_value: u128,
        total_shares: u128,
        total_value: u128,
        right_child: u64,
        xor_map: u64,
    }

    #[constructor]
    fn constructor(ref self: ContractState, _lob_address: ContractAddress) {
        self.lob_address.write(_lob_address);
    }

    #[external(v0)]
    impl HanjiTrie of super::IHanjiTrie<ContractState> {
        fn addOrder(ref self: ContractState, price: u128, amount: u128, owner: ContractAddress) -> felt252 {
            let order_id = generate_order_id(ref self, price);
            let trader_id = get_trader_id(ref self, owner);
            
            let new_leaf = Leaf {
                trader_id: trader_id,
                remain_shares: amount,
                initial_shares: amount,
                initial_value: amount * price,
            };

            let (parent_id, is_left) = find_insertion_point(ref self, order_id);

            if parent_id == 0 {
                self.leaves.write(order_id, new_leaf);
                self.best_offer.write(order_id);
                self.rightmost_map.write(1);
            } else {
                insert_leaf(ref self, parent_id, order_id, new_leaf, is_left, price);
            }

            if order_id < self.best_offer.read() {
                self.best_offer.write(order_id);
            }

            order_id.into()
        }

        fn removeOrder(ref self: ContractState, price: u128, id: felt252) -> (u128, ContractAddress) {
            let order_id: u64 = id.try_into().unwrap();
            let mut leaf = self.leaves.read(order_id);
            
            if leaf.trader_id == 0 {
                return (0, starknet::contract_address_const::<0>());
            }

            let total_shares = leaf.initial_shares;
            let remain_shares = leaf.remain_shares;
            let remain_value = (remain_shares * leaf.initial_value / leaf.initial_shares);

            // Remove the leaf
            self.leaves.write(order_id, Leaf { trader_id: 0, remain_shares: 0, initial_shares: 0, initial_value: 0 });

            let local_best_offer = self.best_offer.read();
            if order_id > local_best_offer {
                return (total_shares, starknet::contract_address_const::<0>());
            }

            if order_id == local_best_offer {
                self.remove_best_offer();
                return (total_shares, self.get_trader_address(leaf.trader_id));
            }

            let (node_id, success) = self.find_rightmost_matching_node(order_id);

            if !success {
                return (total_shares, starknet::contract_address_const::<0>());
            }

            let found = self.remove_order_from_nodes(order_id, node_id, remain_shares, remain_value);
            if found {
                (total_shares, self.get_trader_address(leaf.trader_id))
            } else {
                (total_shares, starknet::contract_address_const::<0>())
            }
        }

        fn claimExecuted(ref self: ContractState, price: u128, id: felt252, amount: u128) -> (u128, u128) {
            let order_id: u64 = id.try_into().unwrap();
            let mut leaf = self.leaves.read(order_id);

            if leaf.trader_id == 0 {
                return (0, 0);
            }

            let total_shares = leaf.initial_shares;
            let mut remain_shares = leaf.remain_shares;
            let local_best_offer = self.best_offer.read();

            if order_id > local_best_offer {
                remain_shares = 0;
            } else {
                let (node_id, success) = self.find_rightmost_matching_node(order_id);
                if !success || !self.is_order_in_tree(node_id, order_id) {
                    remain_shares = 0;
                }
            }

            let executed_shares = total_shares - remain_shares;
            if executed_shares == 0 {
                return (0, remain_shares);
            }

            if remain_shares == 0 {
                // Remove the leaf
                self.leaves.write(order_id, Leaf { trader_id: 0, remain_shares: 0, initial_shares: 0, initial_value: 0 });
            } else {
                let remain_value = (remain_shares * leaf.initial_value) / leaf.initial_shares;
                self.leaves.write(
                    order_id,
                    Leaf {
                        trader_id: leaf.trader_id,
                        remain_shares: remain_shares,
                        initial_shares: leaf.initial_shares,
                        initial_value: leaf.initial_value
                    }
                );
            }

            let executed_value = (executed_shares * leaf.initial_value) / leaf.initial_shares;
            self.update_nodes_after_execution(order_id, executed_shares, executed_value);

            (executed_shares, remain_shares)
        }

        fn executeRight(ref self: ContractState, price: u128, amount: u128) -> (u128, ContractAddress) {
            let local_best_offer = self.best_offer.read();

            // Convert local_best_offer to u128 for comparison
            if price > local_best_offer.into() {
                return (0, starknet::contract_address_const::<0>());
            }

            let mut executed_shares: u128 = 0;
            let mut executed_value: u128 = 0;
            let mut current_id = local_best_offer;

            loop {
                if current_id == 0 || executed_shares == amount {
                    break;
                }

                let leaf = self.leaves.read(current_id);
                let initial_shares = leaf.initial_shares;
                let initial_value = leaf.initial_value;
                let remain_shares = leaf.remain_shares;

                if amount - executed_shares < remain_shares {
                    // Partial execution
                    let exec_shares = amount - executed_shares;
                    let exec_value = (exec_shares * initial_value) / initial_shares;
                    
                    executed_shares += exec_shares;
                    executed_value += exec_value;

                    // Update leaf
                    self.leaves.write(
                        current_id,
                        Leaf {
                            trader_id: leaf.trader_id,
                            remain_shares: remain_shares - exec_shares,
                            initial_shares: initial_shares,
                            initial_value: initial_value
                        }
                    );

                    break;
                } else {
                    // Full execution
                    executed_shares += remain_shares;
                    executed_value += (remain_shares * initial_value) / initial_shares;

                    // Remove leaf
                    self.leaves.write(
                        current_id,
                        Leaf {
                            trader_id: 0,
                            remain_shares: 0,
                            initial_shares: 0,
                            initial_value: 0
                        }
                    );

                    // Move to next leaf
                    current_id = self.find_next_leaf(current_id);
                }
            };

            // Update best offer
            if current_id != local_best_offer {
                self.best_offer.write(current_id);
            }

            // Update nodes
            self.update_nodes_after_execution(local_best_offer, executed_shares, executed_value);

            (executed_shares, self.get_trader_address(self.leaves.read(local_best_offer).trader_id))
        }

        fn getOrderInfo(self: @ContractState, price: u128, id: felt252) -> (u128, ContractAddress) {
            let order_id: u64 = id.try_into().unwrap();
            let leaf = self.leaves.read(order_id);

            if leaf.trader_id == 0 {
                return (0, starknet::contract_address_const::<0>());
            }

            (leaf.remain_shares, self.get_trader_address(leaf.trader_id))
        }

        fn previewExecuteRight(self: @ContractState, price: u128, amount: u128) -> (u128, ContractAddress) {
            let local_best_offer = self.best_offer.read();

            if price > local_best_offer.into() {
                return (0, starknet::contract_address_const::<0>());
            }

            let mut executed_shares: u128 = 0;
            let mut executed_value: u128 = 0;
            let mut current_id = local_best_offer;

            loop {
                if current_id == 0 || executed_shares == amount {
                    break;
                }

                let leaf = self.leaves.read(current_id);
                let initial_shares = leaf.initial_shares;
                let initial_value = leaf.initial_value;
                let remain_shares = leaf.remain_shares;

                if amount - executed_shares < remain_shares {
                    // Partial execution
                    let exec_shares = amount - executed_shares;
                    executed_shares += exec_shares;
                    executed_value += (exec_shares * initial_value) / initial_shares;
                    break;
                } else {
                    // Full execution
                    executed_shares += remain_shares;
                    executed_value += (remain_shares * initial_value) / initial_shares;
                    current_id = self.find_next_leaf(current_id);
                }
            };

            (executed_shares, self.get_trader_address(self.leaves.read(local_best_offer).trader_id))
        }

        fn assembleOrderbookFromOrders(self: @ContractState, price: u128, count: u128) -> Array<(u128, ContractAddress)> {
            let mut result: Array<(u128, ContractAddress)> = ArrayTrait::new();
            let mut current_id = self.best_offer.read();
            let mut remaining_count = count;

            loop {
                if current_id == 0 || remaining_count == 0 {
                    break;
                }

                let leaf = self.leaves.read(current_id);
                if leaf.trader_id != 0 {
                    let trader_address = self.get_trader_address(leaf.trader_id);
                    result.append((leaf.remain_shares, trader_address));
                    remaining_count -= 1;
                }

                current_id = self.find_next_leaf(current_id);
            };

            result
        }
    }

    // Add these methods to the contract implementation
    #[generate_trait]
    impl HanjiTrieImpl of HanjiTrieTrait {
        fn remove_best_offer(ref self: ContractState) {
            let mut best_offer = self.best_offer.read();
            let mut rightmost_map = self.rightmost_map.read();

            if rightmost_map == 1 {
                self.best_offer.write(0);
                self.rightmost_map.write(0);
                return;
            }

            let trailing_zeros = count_trailing_zeros(rightmost_map);
            rightmost_map = rightmost_map - (rightmost_map & (rightmost_map - 1));
            self.rightmost_map.write(rightmost_map);

            best_offer += pow(2, trailing_zeros);
            self.best_offer.write(best_offer);
        }

        fn find_rightmost_matching_node(self: @ContractState, order_id: u64) -> (u64, bool) {
            let mut node_id = self.best_offer.read();
            let mut rightmost_map = self.rightmost_map.read();

            loop {
                if node_id == order_id {
                    break (node_id, true);
                }

                let (key, mask) = extract_key_and_mask_from(node_id);
                if order_id & mask != key {
                    break (node_id, false);
                }

                let shift = count_trailing_zeros(rightmost_map);
                rightmost_map = rightmost_map / pow(2, shift + 1);
                node_id += pow(2, shift);

                if rightmost_map == 0 {
                    break (node_id, false);
                }
            }
        }

        fn get_trader_address(self: @ContractState, trader_id: u64) -> ContractAddress {
            self.trader_addresses.read(trader_id)
        }

        fn remove_order_from_nodes(ref self: ContractState, order_id: u64, mut node_id: u64, shares: u128, value: u128) -> bool {
            loop {
                let mut node = self.nodes.read(node_id);
                node.total_shares -= shares;
                node.total_value -= value;
                self.nodes.write(node_id, node);

                if node_id == order_id {
                    break true; // return true;
                }

                let parent_id = node_id & (node_id - 1);
                if parent_id == 0 {
                    break false; // return false;
                }

                let mut parent = self.nodes.read(parent_id);
                if parent.right == node_id {
                    parent.right = 0;
                } else {
                    parent.left = 0;
                }
                self.nodes.write(parent_id, parent);

                node_id = parent_id;
            };

            false
        }

        fn find_next_leaf(self: @ContractState, current_id: u64) -> u64 {
            let mut next_id = current_id + 2;
            loop {
                if self.leaves.read(next_id).trader_id != 0 {
                    break next_id;
                }
                next_id += 2;
            }
        }

        fn update_nodes_after_execution(
            ref self: ContractState,
            local_best_offer: u64,
            executed_shares: u128,
            executed_value: u128
        ) {
            let mut current_id = local_best_offer;
            loop {
                if current_id == 0 {
                    break;
                }

                let mut node = self.nodes.read(current_id);
                node.total_shares -= executed_shares;
                node.total_value -= executed_value;
                self.nodes.write(current_id, node);

                current_id = current_id & (current_id - 1);
            }
        }

        fn is_order_in_tree(self: @ContractState, node_id: u64, order_id: u64) -> bool {
            if node_id == 0 {
                return false;
            }

            if node_id == order_id {
                return true;
            }

            let node = self.nodes.read(node_id);
            if order_id < node_id {
                self.is_order_in_tree(node.left, order_id)
            } else {
                self.is_order_in_tree(node.right, order_id)
            }
        }
    }

    // Helper functions
    fn generate_order_id(ref self: ContractState, price: u128) -> u64 {
        let mut id: u64 = (price.try_into().unwrap() * 2) + 1;
        loop {
            if self.leaves.read(id).trader_id == 0 {
                break;
            }
            id += 2;
        };
        id
    }

    fn get_trader_id(ref self: ContractState, owner: ContractAddress) -> u64 {
        if self.trader_ids.read(owner) != 0 {
            self.trader_ids.read(owner)
        } else {
            let new_id = self.trader_id_counter.read() + 1;
            self.trader_id_counter.write(new_id);
            self.trader_ids.write(owner, new_id);
            self.trader_addresses.write(new_id, owner);
            new_id
        }
    }

    fn find_insertion_point(ref self: ContractState, order_id: u64) -> (u64, bool) {
        let mut current = self.best_offer.read();
        if current == 0 || order_id < current {
            return (0, false);
        }

        loop {
            let node = self.nodes.read(current);
            if order_id < current {
                if node.left == 0 {
                    break (current, true);
                }
                current = node.left;
            } else {
                if node.right == 0 {
                    break (current, false);
                }
                current = node.right;
            }
        }
    }

    fn insert_leaf(ref self: ContractState, parent_id: u64, order_id: u64, new_leaf: Leaf, is_left: bool, price: u128) {
        self.leaves.write(order_id, new_leaf);

        let mut parent = self.nodes.read(parent_id);
        if is_left {
            parent.left = order_id;
        } else {
            parent.right = order_id;
        }
        parent.total_shares += new_leaf.remain_shares;
        parent.total_value += new_leaf.remain_shares * price;
        self.nodes.write(parent_id, parent);

        if !is_left {
            let current_map = self.rightmost_map.read();
            let shift = 63 - count_trailing_zeros(order_id);
            self.rightmost_map.write(current_map | pow(2, shift));
        }

        update_parent_nodes(ref self, parent_id, new_leaf.remain_shares, new_leaf.remain_shares * price);
    }

    // Helper function to count trailing zeros
    fn count_trailing_zeros(mut x: u64) -> u64 {
        if x == 0 {
            return 64;
        }
        let mut count = 0;
        while (x & 1) == 0 {
            count += 1;
            x = x / 2;
        };
        count
    }

    fn update_parent_nodes(ref self: ContractState, mut node_id: u64, shares: u128, value: u128) {
        while node_id != 0 {
            let mut node = self.nodes.read(node_id);
            node.total_shares += shares;
            node.total_value += value;
            self.nodes.write(node_id, node);
            
            node_id = node_id & (node_id - 1); // Go to parent node
        }
    }

    // Utility functions
    fn extract_key_and_mask_from(node_id: u64) -> (u64, u64) {
        let mask = (node_id - 1) | node_id;
        let key = node_id & mask;
        (key, mask)
    }

    fn extract_key_from(node_id: u64) -> u64 {
        node_id & (node_id - 1)
    }

    fn calc_common_parent(order_id: u64, node_id: u64) -> u64 {
        let key = extract_key_from(node_id);
        let diff = (order_id ^ 1) ^ key;
        let mut mask: u64 = 0xffffffffffffffff;
        let mut bit: u64 = 0x8000000000000000; // Start with the most significant bit

        loop {
            if bit == 0 {
                break;
            }
            if (diff & bit) != 0 {
                break;
            }
            mask = mask & (mask - bit);
            bit = bit / 2;
        };

        let k = (mask & 0x5555555555555555) ^ mask;
        (order_id & mask) ^ k
    }

    fn matches_node(order_id: u64, node_id: u64) -> bool {
        let mask = (node_id - 1) | node_id;
        ((order_id ^ node_id) & mask) == 0
    }

    fn calc_next_right_most_node_id(node_id: u64, map: u64) -> (u64, u64) {
        let mut mask = ~(node_id ^ (node_id - 1));
        let mut filtered_map = map & mask;
        
        if filtered_map == 0 {
            return (0, 0);
        }
        
        mask = ~(filtered_map ^ (filtered_map - 1));
        let k = (mask & 0x5555555555555555) ^ mask;
        
        ((node_id & mask) ^ k, k)
    }

    fn pow(base: u64, exp: u64) -> u64 {
        if exp == 0 {
            1
        } else {
            base * pow(base, exp - 1)
        }
    }

    fn extract_shares_value_for_node(self: @ContractState, node_id: u64) -> (u128, u128) {
        if (node_id & 1) == 1 {
            // leaf case
            let leaf = self.leaves.read(node_id);
            (leaf.remain_shares, leaf.initial_value * leaf.remain_shares / leaf.initial_shares)
        } else {
            let node = self.nodes.read(node_id);
            (node.total_shares, node.total_value)
        }
    }
}
