using System;
using System.Collections.Generic;
using System.Text;
using Lidgren.Network;

namespace Pixel3D.P2P
{
    public static class StringFilter
    {
        public const int MaxNameLength = 30; // <- Needs to be reasonable, because we display it (typing delay)


        static string Truncate(this string s, int maxCharacterCount)
        {
            if(s.Length > maxCharacterCount)
                return s.Substring(0, maxCharacterCount);
            else
                return s;
        }


        static string FilterUnwantedCharacters(this string s)
        {
            // NOTE: This is the ASCII-only fast path that doesn't bring in the character table, and doesn't rebuild the string.

            bool hasSpace = true; // <- block leading spaces (abuse duplicate space checking by assuming we start with one)
            
            for(int i = 0; i < s.Length; i++)
            {
                if(s[i] < 32)
                    return FilterUnwantedCharacters_Rebuild(s); // <- skip right to rebuild, because < 32 is garbage
                else if(s[i] > 126)
                    return FilterUnwantedCharacters_ApproveWithTable(s);
                else if(s[i] == 32) // <- TODO: Other whitespace characters??
                {
                    if(hasSpace) // <- block duplicate spaces
                        return FilterUnwantedCharacters_Rebuild(s);
                    hasSpace = true;
                }
                else // It is an ASCII character
                {
                    hasSpace = false;
                }
            }

            // Block trailing spaces
            if(hasSpace)
                return s.TrimEnd(' ');

            return s;
        }

        private static string FilterUnwantedCharacters_ApproveWithTable(string s)
        {
            // NOTE: Here is where we bring the character table into the cache, but we still don't allocate a StringBuilder to rebuild the string.

            bool hasSpace = true; // <- block leading spaces (abuse duplicate space checking by assuming we start with one)

            for(int i = 0; i < s.Length; i++)
            {
                int index = Array.BinarySearch(ValidCharacters.characters, s[i]);
                if(index < 0)
                    return FilterUnwantedCharacters_Rebuild(s);

                if(char.IsWhiteSpace(s[i])) // <- also bringing in the framework's Unicode character data (PERF: pre-compute? we have like two whitespace characters...)
                {
                    if(hasSpace) // <- block duplicate spaces
                        return FilterUnwantedCharacters_Rebuild(s);
                    hasSpace = true;
                }
                else
                {
                    hasSpace = false;
                }
            }

            // Block trailing spaces
            if(hasSpace)
                return s.TrimEnd(null);

            return s;
        }


        static string FilterUnwantedCharacters_Rebuild(string s)
        {
            // NOTE: This is the slow, allocating path, that rebuilds the string to remove bad characters

            bool hasSpace = true; // <- block leading spaces (abuse duplicate space checking by assuming we start with one)

            StringBuilder sb = new StringBuilder(s.Length);
            for(int i = 0; i < s.Length; i++)
            {
                int index = Array.BinarySearch(ValidCharacters.characters, s[i]);
                if(index < 0)
                    continue;

                if(char.IsWhiteSpace(s[i])) // <- also bringing in the framework's Unicode character data (PERF: pre-compute? we have like two whitespace characters...)
                {
                    if(hasSpace) // <- block duplicate spaces
                        continue;
                    hasSpace = true;
                }
                else
                {
                    hasSpace = false;
                }

                sb.Append(s[i]);
            }

            if(hasSpace && sb.Length > 0)
                sb.Remove(sb.Length - 1, 1);

            return sb.ToString();
        }






        public static string FilterName(this string name)
        {
            name = Truncate(name, MaxNameLength);
            name = FilterUnwantedCharacters(name);

            if(string.IsNullOrWhiteSpace(name))
                name = "????";
            return name;
        }


        #region Name Duplicate Checking

        private static bool HasDuplicates(string name, List<NetConnection> serverConnectionList, string serverLocalName)
        {
            if(string.Equals(name, serverLocalName))
                return true;

            foreach(var connection in serverConnectionList)
            {
                string otherName = (connection.Tag as RemotePeer).PeerInfo.PlayerName;
                if(string.Equals(name, otherName))
                    return true;
            }

            return false;
        }

        internal static string FilterNameNoDuplicates(this string name, List<NetConnection> serverConnectionList, string serverLocalName)
        {
            // Initial fix-up:
            name = name.FilterName();

            if(!HasDuplicates(name, serverConnectionList, serverLocalName))
                return name;

            int number = 1;
            while(true)
            {
                string numberString = " (" + number + ")";
                string extraName = name.Truncate(MaxNameLength - numberString.Length) + numberString;

                if(!HasDuplicates(extraName, serverConnectionList, serverLocalName))
                    return extraName;

                // Try again:
                number++;
            }
        }

        #endregion

    }
}
