public class OctreeIndex implements java.io.Serializable {
	private static final long serialVersionUID = -4178818102103328482L;
	private OctreeNode root;
    private String col1;
    private String col2;
    private String col3;

	public OctreeIndex(OctreeNode root, String col1, String col2, String col3) {
		this.root = root;
		this.col1 = col1;
		this.col2 = col2;
		this.col3 = col3;
	}
	
    public OctreeNode getRoot() {
		return root;
	}

	public void setRoot(OctreeNode root) {
		this.root = root;
	}

	public String getCol1() {
		return col1;
	}

	public void setCol1(String col1) {
		this.col1 = col1;
	}

	public String getCol2() {
		return col2;
	}

	public void setCol2(String col2) {
		this.col2 = col2;
	}

	public String getCol3() {
		return col3;
	}

	public void setCol3(String col3) {
		this.col3 = col3;
	}

	
	public void printAllPoints() {
	    printAllPoints(root);
	}
	
	public void printAllBoundaries() {
		printAllBoundaries(root);
	}

	public void printAllPoints(OctreeNode node) {
	    if (node.getChildren().size()==0) {
	        for (OctreePoint point : node.getPoints()) {
	            System.out.println(point.toString());
	        }
	    } else {
	        for (OctreeNode child : node.getChildren()) {
	            printAllPoints(child);
	        }
	    }
	}
	
	
	public void printAllBoundaries(OctreeNode node) {
		System.out.println(node.getxMin() + " " + node.getxMax());
		System.out.println(node.getyMin() + " " + node.getyMax());
		System.out.println(node.getzMin() + " " + node.getzMax());
	    if (node.getChildren().size()==0) {
	        for (OctreePoint point : node.getPoints()) {
	            System.out.println(point.toString());
	        }
	    } else {
	        for (OctreeNode child : node.getChildren()) {
	        	printAllBoundaries(child);
	        }
	    }
	}
    
   

}


