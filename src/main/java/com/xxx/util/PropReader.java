/*
 * PropReader.java, May 22, 2012
 */
package com.xxx.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;


public final class PropReader
{
    /**
     * Properties Object
     */
    private Properties prop;

    /**
     * Get value corresponding to key from Properties file
     * 
     * @param key
     * @return Null if key doesn't exists, else value corresponding to it
     * @throws Exception
     */
    public String getValue(final String key)
    {
        return prop == null ? null : prop.getProperty(key);
    }

    public PropReader(String FILE_PATH) throws RuntimeException
    {
        FileInputStream propertiesInputStream = null;
        try
        {
            propertiesInputStream = new FileInputStream(FILE_PATH);
            prop = new Properties();
            prop.load(propertiesInputStream);
        }
        catch (FileNotFoundException fnf)
        {
            throw new RuntimeException(fnf.getMessage());
        }
        catch (final Exception exp)
        {
            prop = null;
            throw new RuntimeException(exp.getMessage());
        }
        finally
        {
            if (propertiesInputStream != null)
            {
                try
                {
                    propertiesInputStream.close();
                }
                catch (final IOException ignored)
                {
                }
            }
        }
    }

}