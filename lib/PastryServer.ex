
    
    defmodule PastryServer do
      use GenServer
 
      @globalname :master
      @b 4
     
     def init([nodesNumber, numberOfRequests, numberOfNodesJoined, numberNotInBoth, numRouted, numHops, numRouteNotInBoth]) do
          {:ok, {nodesNumber, [], numberOfRequests, numberOfNodesJoined, numberNotInBoth, numRouted, numHops, numRouteNotInBoth}}
      end
    
      def start_link(nodesNumber, numberOfRequests, numberOfNodesJoined, numberNotInBoth, numRouted, numHops, numRouteNotInBoth) do
        GenServer.start_link(__MODULE__, [nodesNumber, numberOfRequests, numberOfNodesJoined, numberNotInBoth, numRouted, numHops, numRouteNotInBoth])
      end
    
     
      def handle_cast(:start, state) do
        {nodesNumber, _, numberOfRequests, numberOfNodesJoined, numberNotInBoth, numRouted, numHops, numRouteNotInBoth} = state
        numBits = round(Float.ceil(:math.log(nodesNumber)/:math.log(@b)))
        nodeIDSpace = round(Float.ceil(:math.pow(@b, numBits)))
        numFirstGroup = if (nodesNumber <= 1024) do nodesNumber else 1024 end
        randList = Enum.shuffle(Enum.to_list(0..(nodeIDSpace-1)))
        firstGroup = Enum.slice(randList, 0..(numFirstGroup-1))
    
        list_pid = for nodeID <- firstGroup do
          {_, pid} = Pastry.startlink(nodeID, nodesNumber)
          pid
        end 
        for pid <- list_pid do
          GenServer.cast(pid, {:first_join, firstGroup})
        end
        {:noreply, {nodesNumber, randList, numberOfRequests, numberOfNodesJoined, numberNotInBoth, numRouted, numHops, numRouteNotInBoth}}
      end
    
      def handle_cast(:join_finish, state) do
        {nodesNumber, randList, numberOfRequests, numberOfNodesJoined, numberNotInBoth, numRouted, numHops, numRouteNotInBoth} = state
        numFirstGroup = if (nodesNumber <= 1024) do nodesNumber else 1024 end
        numberOfNodesJoined = numberOfNodesJoined + 1
        if(numberOfNodesJoined >= numFirstGroup) do
          if(numberOfNodesJoined >= nodesNumber) do
            GenServer.cast(:global.whereis_name(@globalname), :begin_route) 
          else
            GenServer.cast(:global.whereis_name(@globalname), :second_join) #This call is made when the number of nodes is greated than 1024. 
          end
        end
        {:noreply, {nodesNumber, randList, numberOfRequests, numberOfNodesJoined, numberNotInBoth, numRouted, numHops, numRouteNotInBoth}}
      end
    
        def handle_cast({:route_finish, fromID, toID, hops}, state) do
        {nodesNumber, randList, numberOfRequests, numberOfNodesJoined, numberNotInBoth, numRouted, numHops, numRouteNotInBoth} = state
        numRouted = numRouted + 1
        if hops < 0 do
           IO.inspect "Negative hops shouldn't be allowed."
        end
        numHops = numHops + hops
        if (numRouted >= nodesNumber * numberOfRequests) do
          IO.puts "Average number of hops are  #{numHops/numRouted}"
          Process.exit(self(), :processover)
        end
        {:noreply, {nodesNumber, randList, numberOfRequests, numberOfNodesJoined, numberNotInBoth, numRouted, numHops, numRouteNotInBoth}}
      end
    
      def handle_cast(:begin_route, state) do
        {_, randList, numberOfRequests, _, _, _, _, _} = state
        for node <- randList do
            GenServer.cast(String.to_atom("Node"<>Integer.to_string(node)), {:begin_route, numberOfRequests})
        end
        {:noreply, state}
      end
    
        def handle_cast(:second_join, state) do
        {nodesNumber, randList, numberOfRequests, numberOfNodesJoined, numberNotInBoth, numRouted, numHops, numRouteNotInBoth} = state
        startID = Enum.at(randList, Enum.random(0..(numberOfNodesJoined-1)))
        Pastry.startlink(Enum.at(randList, numberOfNodesJoined), nodesNumber)
        GenServer.cast(String.to_atom("Node"<>Integer.to_string(startID)), {:route, "Join", startID, Enum.at(randList, numberOfNodesJoined), 0})
        {:noreply, {nodesNumber, randList, numberOfRequests, numberOfNodesJoined, numberNotInBoth, numRouted, numHops, numRouteNotInBoth}}
      end
    
      def main(args) do
        [nodesNumber, numberOfRequests] = args
        nodesNumber = String.to_integer(nodesNumber)
        numberOfRequests = String.to_integer(numberOfRequests)
        numRouteNotInBoth = 0
        numberOfNodesJoined = 0
        numberNotInBoth = 0
        numRouted = 0
        numHops = 0
        {:ok, master_pid} = start_link(nodesNumber, numberOfRequests, numberOfNodesJoined, numberNotInBoth, numRouted, numHops, numRouteNotInBoth)
        :global.register_name(@globalname, master_pid)
        :global.sync()
        GenServer.cast(:global.whereis_name(@globalname), :start)
        
        :timer.sleep(:infinity)
      end
    end
    
    