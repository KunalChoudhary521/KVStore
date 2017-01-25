package cache;

import cache.Node;

public class CacheQueue/*DoublyLinkedList*/
{
    private Node head;
    private Node tail;
    private int size;

    public CacheQueue()
    {
        this.head = null;
        this.tail = null;
        this.size = 0;
    }

    public Node getHead() { return this.head; }
    public void setHead(Node hd){ this.head = hd; }

    public Node getTail() { return this.tail; }
    public void setTail(Node tl) { this.tail = tl; }

    public int getSize() { return this.size;}
    //we don't want user to change size, this class will handle that

    public boolean isEmpty()
    {
        if((this.size == 0) && (this.head == null) && (this.tail == null))
        {
            return true;
        }
        return false;
    }
    public void insertToFront(Node nd)//add nd to the front of Queue
    {
        //intitally, nd is not connected to any other node
        if(this.isEmpty())
        {
            this.head = nd;
            this.tail = nd;
            //nd.prev is already null
        }
        else
        {
            nd.setNext(this.head);
            this.head.setPrev(nd);
            this.head = nd;

            nd.setPrev(null);
        }

        this.size++;
    }

    public void remove(Node ndToDelete)
    {
        //3 Cases
        //Case 1: CacheQueue is empty
        if(this.isEmpty())
        {
            return;
        }
        //Case 2: ndToDelete is the last node in the CacheQueue
        else if(ndToDelete.equals(this.getTail()))
        {
            this.tail = ndToDelete.getPrev();
            this.tail.setNext(null);

            ndToDelete.setPrev(null);
            ndToDelete.setNext(null);
        }
        //Case 3: ndToDelete is somewhere in the middle
        else
        {
            Node tempNext = ndToDelete.getNext();
            Node tempPrev = ndToDelete.getPrev();

            tempPrev.setNext(tempNext);
            tempNext.setPrev(tempPrev);

            ndToDelete.setPrev(null);
            ndToDelete.setNext(null);

        }
        this.size--;
    }

    public void printList()
    {
        System.out.print(this.head.getKey() + ":" + this.head.getValue());

        Node temp = this.head.getNext();
        while(temp != null)
        {
            System.out.print("->" + temp.getKey() + ":" + temp.getValue());
            temp = temp.getNext();
        }

        System.out.println();
    }

    //public void deleteQueue();//not sure if this is necessary
}
