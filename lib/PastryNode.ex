defmodule Pastry do
  use GenServer
  
      @name :master
      @b 4
  
  def init([id, nodes]) do
        bits = round(Float.ceil(:math.log(nodes)/:math.log(@b))) 
        row = Tuple.duplicate(-1, @b) 
        routingTable = Tuple.duplicate(row, bits)
        prev = 0
        {:ok, {id, nodes, [], [], routingTable, prev}}
      end
  
      def startlink(id, nodes) do
        name = String.to_atom("Node"<>Integer.to_string(id))
        GenServer.start_link(__MODULE__, [id, nodes], name: name)
      end
  
      def toBase(id, len) do
        nodeID = Integer.to_string(id, @b)
        String.pad_leading(nodeID, len, "0")
      end
  
      def addToBuffer(my, first, bits, small, large, routingTable) do
        if length(first) == 0 do
          {small, large, routingTable}
        else
          id = List.first(first)
          
          large = if (id > my && !Enum.member?(large, id)) do #This is for the larger leaf
            if(length(large) < 4) do
              large ++ [id]
            else
              if (id < Enum.max(large)) do
                large = List.delete(large, Enum.max(large))
                large ++ [id]
              else
                large
              end
            end
          else
            large
          end
          small = if (id < my && !Enum.member?(small, id)) do 
            if(length(small) < 4) do
              small ++ [id]
            else
              if (id > Enum.min(small)) do
                small = List.delete(small, Enum.min(small))
                small ++ [id]
              else
                small
              end
            end
          else
            small
          end
        
          pref = checkPrefix(toBase(my, bits), toBase(id, bits), 0)
          next = String.to_integer(String.at(toBase(id, bits), pref))
          routingTable = if elem(elem(routingTable, pref), next) == -1 do
            row = elem(routingTable, pref)
            updatedRow = Tuple.insert_at(Tuple.delete_at(row, next), next, id)
            Tuple.insert_at(Tuple.delete_at(routingTable, pref), pref, updatedRow)
          else
            routingTable
          end
            {_, _, _} = addToBuffer(my, List.delete_at(first, 0), bits, small, large, routingTable)
        end
      end

  
      def findRouteNodes(routingTable, i, j, bits, my, prev) do
      if i >= bits or j >= 4 do
        prev
      else
         node = elem(elem(routingTable, i), j)
         if node != -1 do
          prev=prev+1
              GenServer.cast(String.to_atom("Node"<>Integer.to_string(node)), {:update, my})
         end
         prev = findRouteNodes(routingTable, i, j + 1, bits, my, prev)
         if j == 0 do
          prev = findRouteNodes(routingTable, i + 1, j, bits, my, prev)
         end
         prev
      end
      end


      def checkPrefix(id1, id2, pos) do
        if String.first(id1) != String.first(id2) do
          pos
        else
          checkPrefix(String.slice(id1, 1..(String.length(id1)-1)), String.slice(id2, 1..(String.length(id2)-1)), pos+1)
        end   
      end

  
      def addNewRow(routingTable, rowNum, newRow, _) do
        routingTable = Tuple.insert_at(Tuple.delete_at(routingTable, rowNum), rowNum, newRow)
      end
  
      def sendReq([_ | rest], my, listNodes) do
          Process.sleep(2000)
          listneigh = Enum.to_list(0..listNodes-1)
          destination = Enum.random(List.delete(listneigh, my))
          if destination == my do
            IO.inspect "wrong destination"
          end
          GenServer.cast(String.to_atom("Node"<>Integer.to_string(my)), {:route, "Route", my, destination, 0})
          sendReq(rest, my, listNodes)
      end
  
     
  
      def handle_cast({:first_join, first}, state) do
        {my, nodes, small, large, routingTable, prev} = state
        bits = round(Float.ceil(:math.log(nodes)/:math.log(@b)))
        first = List.delete(first, my)
        {small, large, routingTable} = addToBuffer(my, first, bits, small, large, routingTable)
  
        for i <- 0..(bits-1) do
          next = String.to_integer(String.at(toBase(my, bits), i))
          row = elem(routingTable, i)
          updatedRow = Tuple.insert_at(Tuple.delete_at(row, next), next, my)
          Tuple.insert_at(Tuple.delete_at(routingTable, i), i, updatedRow)
        end
  
        GenServer.cast(:global.whereis_name(@name), :join_finish)
        {:noreply, {my, nodes, small, large, routingTable, prev}}
      end
  
      def sendReq([], _, _) do
        {:ok}
       end

      def handle_cast({:route, msg,from, to, hops}, state) do
        {my, nodes, small, large, routingTable, prev} = state
        bits = round(Float.ceil(:math.log(nodes)/:math.log(@b)))
        listNodes = round(Float.ceil(:math.pow(@b, bits)))
        
       if  msg=="Join" do
        pref = checkPrefix(toBase(my, bits), toBase(to, bits), 0)
            next = String.to_integer(String.at(toBase(to, bits), pref))
            if(hops == 0 && pref > 0) do
              for i <- 0..(pref-1) do
              GenServer.cast(String.to_atom("Node"<>Integer.to_string(to)), {:addRow, i, elem(routingTable,i)})
              end
            end
            GenServer.cast(String.to_atom("Node"<>Integer.to_string(to)), {:addRow, pref, elem(routingTable, pref)})
  
          cond do
            (length(small)>0 && to >= Enum.min(small) && to <= my) || (length(large)>0 && to <= Enum.max(large) && to >= my) ->        
              diff=listNodes + 10
              closest=-1
              closest = if(to < my) do
              for i<-small do
                if(abs(to - i) < diff) do
                  closest=i
                end
              end
              else 
                for i<-large do
                  if(abs(to - i) < diff) do
                    closest=i
                    diff=abs(to-i)
                  end
                end
                closest
              end
              if(abs(to - my) > diff) do
                GenServer.cast(String.to_atom("Node"<>Integer.to_string(closest)), {:route,msg,from,to,hops+1}) 
              else
                leaves = []
                leaves ++ [my]++[small]++[large] 
                GenServer.cast(String.to_atom("Node"<>Integer.to_string(to)), {:add_leaf,leaves})
              end  
            length(small)<4 && length(small)>0 && to < Enum.min(small) ->
              GenServer.cast(String.to_atom("Node"<>Integer.to_string(Enum.min(small))), {:route,msg,from,to,hops+1})
            length(large)<4 && length(large)>0 && to > Enum.max(large) ->
              GenServer.cast(String.to_atom("Node"<>Integer.to_string(Enum.max(large))), {:route,msg,from,to,hops+1})
            (length(small)==0 && to<my) || (length(large)==0 && to>my) -> 
            leaves = []
            leaves ++ [my]++[small]++[large]
              GenServer.cast(String.to_atom("Node"<>Integer.to_string(to)), {:add_leaf,leaves})
            elem(elem(routingTable, pref), next) != -1 ->
              GenServer.cast(String.to_atom("Node"<>Integer.to_string(elem(elem(routingTable, pref), next))), {:route,msg,from,to,hops+1})
              to > my ->
              GenServer.cast(String.to_atom("Node"<>Integer.to_string(Enum.max(large))), {:route,msg,from,to,hops+1})
  
              to < my ->
              GenServer.cast(String.to_atom("Node"<>Integer.to_string(Enum.min(small))), {:route,msg,from,to,hops+1})
            true ->
              IO.puts("Impossible")
          end
       else

            if my == to do
              GenServer.cast(:global.whereis_name(@name), {:route_finish,from,to,hops+1})

            else 
              pref = checkPrefix(toBase(my, bits), toBase(to, bits), 0)
              next = String.to_integer(String.at(toBase(to, bits), pref))
            
              cond do
              (length(small)>0 && to >= Enum.min(small) && to < my) || (length(large)>0 && to <= Enum.max(large) && to > my) ->
                diff=listNodes + 10
                closest=-1
                closest = if(to < my) do
                  for i<-small do
                    if(abs(to - i) < diff) do
                      closest=i
                      diff=abs(to-i)
                    end
                  end
                else 
                  for i<-large do
                      if(abs(to - i) < diff) do
                        closest=i
                        diff=abs(to-i)
                      end
                  end
                  closest
                end
  
                if(abs(to - my) > diff) do
                  GenServer.cast(String.to_atom("Node"<>Integer.to_string(closest)), {:route,"Route",from,to,hops+1})
                else
                  GenServer.cast(:global.whereis_name(@name), {:route_finish,from,to,hops+1})
                end                      
                
                length(small)<4 && length(small)>0 && to < Enum.min(small) ->
                  GenServer.cast(String.to_atom("Node"<>Integer.to_string(Enum.min(small))), {:route,"Route",from,to,hops+1})
                length(large)<4 && length(large)>0 && to > Enum.max(large) ->
                  GenServer.cast(String.to_atom("Node"<>Integer.to_string(Enum.max(large))), {:route,"Route",from,to,hops+1})
                (length(small)==0 && to<my) || (length(large)==0 && to>my) -> 
                  GenServer.cast(:global.whereis_name(@name), {:route_finish,from,to,hops+1})
                 elem(elem(routingTable, pref), next) != -1 ->
                  GenServer.cast(String.to_atom("Node"<>Integer.to_string(elem(elem(routingTable, pref), next))), {:route,"Route",from,to,hops+1})
                  to > my ->
                  GenServer.cast(String.to_atom("Node"<>Integer.to_string(Enum.max(large))), {:route,"Route",from,to,hops+1})
                  to < my ->
                  GenServer.cast(String.to_atom("Node"<>Integer.to_string(Enum.min(small))), {:route,"Route",from,to,hops+1})
                true ->
                  IO.puts("Impossible")
            end      
          end 
        end 
        {:noreply, {my, nodes, small, large, routingTable, prev}}
      end
      
     
      def handle_cast({:begin_route, numberOfRequests}, state) do
        {my, nodes, small, large, routingTable, prev} = state
        bits = round(Float.ceil(:math.log(nodes)/:math.log(@b)))
        listNodes = round(Float.ceil(:math.pow(@b, bits)))
        sendReq(Enum.to_list(1..numberOfRequests), my, listNodes)
        {:noreply, {my, nodes, small, large, routingTable, prev}}
      end
  
      def handle_cast(:acknowledgement, state) do
        {my, nodes, small, large, routingTable, prev} = state
        prev = prev - 1
        if(prev == 0) do
          GenServer.cast(:global.whereis_name(@name), :join_finish)
        end
        {:noreply, {my, nodes, small, large, routingTable, prev}}
      end

      def handle_cast({:update, newNode}, state) do
        {my, nodes, small, large, routingTable, prev} = state
        bits = round(Float.ceil(:math.log(nodes)/:math.log(@b)))
        {small, large, routingTable} = addToBuffer(my, [newNode], bits, small, large, routingTable)
        GenServer.cast(String.to_atom("Node"<>Integer.to_string(newNode)), :acknowledgement)
        {:noreply, {my, nodes, small, large, routingTable, prev}}
      end

      def handle_cast({:addRow,rowNum,newRow}, state) do 
          {my, nodes, small, large, routingTable, prev} = state
          routingTable =  Tuple.insert_at(Tuple.delete_at(routingTable, rowNum), rowNum, newRow)  
          {:noreply, {my, nodes, small, large, routingTable, prev}}
      end
   
  
      def handle_cast({:add_leaf, leaves}, state) do
        {my, nodes, small, large, routingTable, prev} = state
        bits = round(Float.ceil(:math.log(nodes)/:math.log(@b)))
        {small, large, routingTable} = addToBuffer(my, leaves, bits, small, large, routingTable)
        for i <- small do
              GenServer.cast(String.to_atom("Node"<>Integer.to_string(i)), {:update, my})
        end
        for i <- large do
              GenServer.cast(String.to_atom("Node"<>Integer.to_string(i)), {:update, my})
        end
        prev = prev + length(small) + length(large)
        prev = findRouteNodes(routingTable, 0, 0, bits, my, prev)
        for i <- 0..(bits-1) do
          for j <- 0..3 do
            row = elem(routingTable, i)
            updatedRow = Tuple.insert_at(Tuple.delete_at(row, j), j, my)
            Tuple.insert_at(Tuple.delete_at(routingTable, i), i, updatedRow)
          end
        end
        {:noreply, {my, nodes, small, large, routingTable, prev}}
      end
  end