package org.ergemp.toolkit.hadoop.utils;

import java.util.Iterator;
import java.util.regex.Pattern;
import org.apache.hadoop.io.Text;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

public class JsonParser {
    public String line = "";
    public String filter = "";
    public String value = "";
    public String key = "";
    public Boolean search = false;

    public void parse(String gStr, String gKey, String gFilter)
    {
        this.line = gStr;
        this.key = gKey;
        this.value = "";
        this.filter = gFilter;

        try
        {
            searchInner(gStr);
        }
        catch(Exception ex)
        {
            System.out.println(ex.getMessage());
        }
        finally
        {
        }
    }

    public String searchInner(String gStr)
    {
        String retVal = "";
        try
        {
            JSONParser parser = new JSONParser();
            JSONObject obj = (JSONObject)parser.parse(gStr);

            //if (Pattern.matches(filter, gStr))
            if(true)
            {

                if (obj.containsKey(this.key))
                {
                    //if the root json object has the key
                    //
                    retVal = obj.get(this.key).toString();

                    if (value.equalsIgnoreCase(""))
                    {
                        //set the value of the class if not set before
                        value = retVal.replaceAll("[\\[\\]]+", "");
                    }
                    else
                    {
                        //add the value of the class
                        //if there is more than one apperance of the same key
                        //resolve the csv within the map
                        value = value + "," + retVal.replaceAll("[\\[\\]]+", "");
                    }
                }
                else
                {
                    //if the root object doesnt have the filter key
                    //then maybe the key exists in the sub objects or arrays
                    //iterate over them
                    Iterator it = obj.keySet().iterator();

                    while (it.hasNext())
                    {
                        String next = (String)it.next().toString();

                        if (obj.get(next) == null)
                        {
                            continue;
                        }

                        //if the value of the key is an object
                        //call the recursive function with the string value of the key
                        if (isObject(obj.get(next).toString()))
                        {
                            searchInner(obj.get(next).toString());
                        }
                        //if the value is an array
                        //send all the array elements as the string rep. of the object
                        else if (isArray(obj.get(next).toString()))
                        {
                            JSONArray arr = (JSONArray)parser.parse(obj.get(next).toString());

                            Iterator it2 = arr.iterator();
                            while (it2.hasNext())
                            {
                                JSONObject arrObj = (JSONObject)it2.next();
                                searchInner(arrObj.toString());
                            }
                        }
                        else
                        {
                            //means the value is not an object or an array
                            //dont iterate on values
                        }
                    }
                }
            }
        }
        catch(Exception ex)
        {
            System.out.println(ex.toString());
            ex.printStackTrace();
            return "";
        }
        finally
        {
            return retVal;
        }
    }

    public Boolean isArray(String gStr)
    {
        Boolean retVal = false;
        try
        {
            JSONParser parser = new JSONParser();
            JSONArray arr = (JSONArray)parser.parse(gStr);
            retVal = true;
        }
        catch(Exception ex)
        {
            retVal = false;
        }
        finally
        {
            return retVal;
        }
    }

    public Boolean isObject(String gStr)
    {
        Boolean retVal = false;
        try
        {
            JSONParser parser = new JSONParser();
            JSONObject obj = (JSONObject)parser.parse(gStr);
            retVal = true;
        }
        catch(Exception ex)
        {
            retVal = false;
        }
        finally
        {
            return retVal;
        }
    }

    public Boolean filter()
    {
        if (Pattern.matches(filter, line))
        {
            this.search = true;
            return true;
        }
        else
        {
            this.search = false;
            return false;
        }
    }

    public void regexFilter(String gFilter)
    {


    }

    public void parse(Text gText, String gKey, String gFilter)
    {
        this.parse(gText.toString(), gKey, gFilter);
    }

    public String getKey()
    {
        return this.key;
    }

    public String getValue()
    {
        return this.value;
    }
}
