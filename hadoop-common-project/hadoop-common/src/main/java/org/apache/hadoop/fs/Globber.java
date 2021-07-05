/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.DurationInfo;

import org.apache.hadoop.tracing.TraceScope;
import org.apache.hadoop.tracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of {@link FileSystem#globStatus(Path, PathFilter)}.
 * This has historically been package-private; it has been opened
 * up for object stores within the {@code hadoop-*} codebase ONLY.
 * It could be expanded for external store implementations in future.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Globber {
  public static final Logger LOG =
      LoggerFactory.getLogger(Globber.class.getName());

  private final FileSystem fs;
  private final FileContext fc;
  private final Path pathPattern;
  private final PathFilter filter;
  private final Tracer tracer;
  private final boolean resolveSymlinks;

  Globber(FileSystem fs, Path pathPattern, PathFilter filter) {
    this.fs = fs;
    this.fc = null;
    this.pathPattern = pathPattern;
    this.filter = filter;
    this.tracer = FsTracer.get(fs.getConf());
    this.resolveSymlinks = true;
  }

  Globber(FileContext fc, Path pathPattern, PathFilter filter) {
    this.fs = null;
    this.fc = fc;
    this.pathPattern = pathPattern;
    this.filter = filter;
    this.tracer = fc.getTracer();
    this.resolveSymlinks = true;
  }

  /**
   * Filesystem constructor for use by {@link GlobBuilder}.
   * @param fs filesystem
   * @param pathPattern path pattern
   * @param filter optional filter
   * @param resolveSymlinks should symlinks be resolved.
   */
  private Globber(FileSystem fs, Path pathPattern, PathFilter filter,
      boolean resolveSymlinks) {
    this.fs = fs;
    this.fc = null;
    this.pathPattern = pathPattern;
    this.filter = filter;
    this.resolveSymlinks = resolveSymlinks;
    this.tracer = FsTracer.get(fs.getConf());
    LOG.debug("Created Globber for path={}, symlinks={}",
        pathPattern, resolveSymlinks);
  }

  /**
   * File Context constructor for use by {@link GlobBuilder}.
   * @param fc file context
   * @param pathPattern path pattern
   * @param filter optional filter
   * @param resolveSymlinks should symlinks be resolved.
   */
  private Globber(FileContext fc, Path pathPattern, PathFilter filter,
      boolean resolveSymlinks) {
    this.fs = null;
    this.fc = fc;
    this.pathPattern = pathPattern;
    this.filter = filter;
    this.resolveSymlinks = resolveSymlinks;
    this.tracer = fc.getTracer();
    LOG.debug("Created Globber path={}, symlinks={}",
        pathPattern, resolveSymlinks);
  }

  private FileStatus getFileStatus(Path path) throws IOException {
    try {
      if (fs != null) {
        return fs.getFileStatus(path);
      } else {
        return fc.getFileStatus(path);
      }
    } catch (FileNotFoundException e) {
      LOG.debug("getFileStatus({}) failed; returning null", path, e);
      return null;
    }
  }

  private FileStatus[] listStatus(Path path) throws IOException {
    try {
      if (fs != null) {
        return fs.listStatus(path);
      } else {
        return fc.util().listStatus(path);
      }
    } catch (FileNotFoundException e) {
      LOG.debug("listStatus({}) failed; returning empty array", path, e);
      return new FileStatus[0];
    }
  }

  private Path fixRelativePart(Path path) {
    if (fs != null) {
      return fs.fixRelativePart(path);
    } else {
      return fc.fixRelativePart(path);
    }
  }

  /**
   * Convert a path component that contains backslash ecape sequences to a
   * literal string.  This is necessary when you want to explicitly refer to a
   * path that contains globber metacharacters.
   */
  private static String unescapePathComponent(String name) {
    return name.replaceAll("\\\\(.)", "$1");
  }

  /**
   * Translate an absolute path into a list of path components.
   * We merge double slashes into a single slash here.
   * POSIX root path, i.e. '/', does not get an entry in the list.
   */
  private static List<String> getPathComponents(String path)
      throws IOException {
    ArrayList<String> ret = new ArrayList<>();
    for (String component : path.split(Path.SEPARATOR)) {
      if (!component.isEmpty()) {
        ret.add(component);
      }
    }
    return ret;
  }

  private String schemeFromPath(Path path) throws IOException {
    String scheme = path.toUri().getScheme();
    if (scheme == null) {
      if (fs != null) {
        scheme = fs.getUri().getScheme();
      } else {
        scheme = fc.getFSofPath(fc.fixRelativePart(path)).
                    getUri().getScheme();
      }
    }
    return scheme;
  }

  private String authorityFromPath(Path path) throws IOException {
    String authority = path.toUri().getAuthority();
    if (authority == null) {
      if (fs != null) {
        authority = fs.getUri().getAuthority();
      } else {
        authority = fc.getFSofPath(fc.fixRelativePart(path)).
                      getUri().getAuthority();
      }
    }
    return authority ;
  }

  public FileStatus[] glob() throws IOException {
    TraceScope scope = tracer.newScope("Globber#glob");
    scope.addKVAnnotation("pattern", pathPattern.toUri().getPath());
    try (DurationInfo ignored = new DurationInfo(LOG, false,
        "glob %s", pathPattern)) {
      return doGlob();
    } finally {
      scope.close();
    }
  }

  private FileStatus[] doGlob() throws IOException {
    // First we get the scheme and authority of the pattern that was passed
    // in.
    String scheme = schemeFromPath(pathPattern);
    String authority = authorityFromPath(pathPattern);

    // Next we strip off everything except the pathname itself, and expand all
    // globs.  Expansion is a process which turns "grouping" clauses,
    // expressed as brackets, into separate path patterns.
    String pathPatternString = pathPattern.toUri().getPath();
    List<String> flattenedPatterns = GlobExpander.expand(pathPatternString);

    LOG.debug("Filesystem glob {}", pathPatternString);
    // Now loop over all flattened patterns.  In every case, we'll be trying to
    // match them to entries in the filesystem.
    ArrayList<FileStatus> results = 
        new ArrayList<>(flattenedPatterns.size());
    boolean sawWildcard = false;
    for (String flatPattern : flattenedPatterns) {
      // Get the absolute path for this flattened pattern.  We couldn't do 
      // this prior to flattening because of patterns like {/,a}, where which
      // path you go down influences how the path must be made absolute.
      Path absPattern = fixRelativePart(new Path(
          flatPattern.isEmpty() ? Path.CUR_DIR : flatPattern));
      LOG.debug("Pattern: {}", absPattern);
      // Now we break the flattened, absolute pattern into path components.
      // For example, /a/*/c would be broken into the list [a, *, c]
      List<String> components =
          getPathComponents(absPattern.toUri().getPath());
      // Starting out at the root of the filesystem, we try to match
      // filesystem entries against pattern components.
      ArrayList<FileStatus> candidates = new ArrayList<>(1);
      // To get the "real" FileStatus of root, we'd have to do an expensive
      // RPC to the NameNode.  So we create a placeholder FileStatus which has
      // the correct path, but defaults for the rest of the information.
      // Later, if it turns out we actually want the FileStatus of root, we'll
      // replace the placeholder with a real FileStatus obtained from the
      // NameNode.
      FileStatus rootPlaceholder;
      if (Path.WINDOWS && !components.isEmpty()
          && Path.isWindowsAbsolutePath(absPattern.toUri().getPath(), true)) {
        // On Windows the path could begin with a drive letter, e.g. /E:/foo.
        // We will skip matching the drive letter and start from listing the
        // root of the filesystem on that drive.
        String driveLetter = components.remove(0);
        rootPlaceholder = new FileStatus(0, true, 0, 0, 0, new Path(scheme,
            authority, Path.SEPARATOR + driveLetter + Path.SEPARATOR));
      } else {
        rootPlaceholder = new FileStatus(0, true, 0, 0, 0,
            new Path(scheme, authority, Path.SEPARATOR));
      }
      candidates.add(rootPlaceholder);
      
      for (int componentIdx = 0; componentIdx < components.size();
          componentIdx++) {
        ArrayList<FileStatus> newCandidates =
            new ArrayList<>(candidates.size());
        GlobFilter globFilter = new GlobFilter(components.get(componentIdx));
        String component = unescapePathComponent(components.get(componentIdx));
        if (globFilter.hasPattern()) {
          sawWildcard = true;
        }
        LOG.debug("Component {}, patterned={}", component, sawWildcard);
        if (candidates.isEmpty() && sawWildcard) {
          // Optimization: if there are no more candidates left, stop examining 
          // the path components.  We can only do this if we've already seen
          // a wildcard component-- otherwise, we still need to visit all path 
          // components in case one of them is a wildcard.
          break;
        }
        if ((componentIdx < components.size() - 1) &&
            (!globFilter.hasPattern())) {
          // Optimization: if this is not the terminal path component, and we 
          // are not matching against a glob, assume that it exists.  If it 
          // doesn't exist, we'll find out later when resolving a later glob
          // or the terminal path component.
          for (FileStatus candidate : candidates) {
            candidate.setPath(new Path(candidate.getPath(), component));
          }
          continue;
        }
        for (FileStatus candidate : candidates) {
          if (globFilter.hasPattern()) {
            FileStatus[] children = listStatus(candidate.getPath());
            if (children.length == 1) {
              // If we get back only one result, this could be either a listing
              // of a directory with one entry, or it could reflect the fact
              // that what we listed resolved to a file.
              //
              // Unfortunately, we can't just compare the returned paths to
              // figure this out.  Consider the case where you have /a/b, where
              // b is a symlink to "..".  In that case, listing /a/b will give
              // back "/a/b" again.  If we just went by returned pathname, we'd
              // incorrectly conclude that /a/b was a file and should not match
              // /a/*/*.  So we use getFileStatus of the path we just listed to
              // disambiguate.
              if (resolveSymlinks) {
                LOG.debug("listStatus found one entry; disambiguating {}",
                    children[0]);
                Path path = candidate.getPath();
                FileStatus status = getFileStatus(path);
                if (status == null) {
                  // null means the file was not found
                  LOG.warn("File/directory {} not found:"
                      + " it may have been deleted."
                      + " If this is an object store, this can be a sign of"
                      + " eventual consistency problems.",
                      path);
                  continue;
                }
                if (!status.isDirectory()) {
                  LOG.debug("Resolved entry is a file; skipping: {}", status);
                  continue;
                }
              } else {
                // there's no symlinks in this store, so no need to issue
                // another call, just see if the result is a directory or a file
                if (children[0].getPath().equals(candidate.getPath())) {
                  // the listing status is of a file
                  continue;
                }
              }
            }
            for (FileStatus child : children) {
              if (componentIdx < components.size() - 1) {
                // Don't try to recurse into non-directories.  See HADOOP-10957.
                if (!child.isDirectory()) continue; 
              }
              // Set the child path based on the parent path.
              child.setPath(new Path(candidate.getPath(),
                      child.getPath().getName()));
              if (globFilter.accept(child.getPath())) {
                newCandidates.add(child);
              }
            }
          } else {
            // When dealing with non-glob components, use getFileStatus 
            // instead of listStatus.  This is an optimization, but it also
            // is necessary for correctness in HDFS, since there are some
            // special HDFS directories like .reserved and .snapshot that are
            // not visible to listStatus, but which do exist.  (See HADOOP-9877)
            FileStatus childStatus = getFileStatus(
                new Path(candidate.getPath(), component));
            if (childStatus != null) {
              newCandidates.add(childStatus);
            }
          }
        }
        candidates = newCandidates;
      }
      for (FileStatus status : candidates) {
        // Use object equality to see if this status is the root placeholder.
        // See the explanation for rootPlaceholder above for more information.
        if (status == rootPlaceholder) {
          status = getFileStatus(rootPlaceholder.getPath());
          if (status == null) continue;
        }
        // HADOOP-3497 semantics: the user-defined filter is applied at the
        // end, once the full path is built up.
        if (filter.accept(status.getPath())) {
          results.add(status);
        }
      }
    }
    /*
     * When the input pattern "looks" like just a simple filename, and we
     * can't find it, we return null rather than an empty array.
     * This is a special case which the shell relies on.
     *
     * To be more precise: if there were no results, AND there were no
     * groupings (aka brackets), and no wildcards in the input (aka stars),
     * we return null.
     */
    if ((!sawWildcard) && results.isEmpty() &&
        (flattenedPatterns.size() <= 1)) {
      LOG.debug("No matches found and there was no wildcard in the path {}",
          pathPattern);
      return null;
    }
    /*
     * In general, the results list will already be sorted, since listStatus
     * returns results in sorted order for many Hadoop filesystems.  However,
     * not all Hadoop filesystems have this property.  So we sort here in order
     * to get consistent results.  See HADOOP-10798 for details.
     */
    FileStatus ret[] = results.toArray(new FileStatus[0]);
    Arrays.sort(ret);
    return ret;
  }

  /**
   * Create a builder for a Globber, bonded to the specific filesystem.
   * @param filesystem filesystem
   * @return the builder to finish configuring.
   */
  public static GlobBuilder createGlobber(FileSystem filesystem) {
    return new GlobBuilder(filesystem);
  }

  /**
   * Create a builder for a Globber, bonded to the specific file
   * context.
   * @param fileContext file context.
   * @return the builder to finish configuring.
   */
  public static GlobBuilder createGlobber(FileContext fileContext) {
    return new GlobBuilder(fileContext);
  }

  /**
   * Builder for Globber instances.
   */
  @InterfaceAudience.Private
  public static class GlobBuilder {

    private final FileSystem fs;

    private final FileContext fc;

    private Path pathPattern;

    private PathFilter filter;

    private boolean resolveSymlinks = true;

    /**
     * Construct bonded to a file context.
     * @param fc file context.
     */
    public GlobBuilder(final FileContext fc) {
      this.fs = null;
      this.fc = checkNotNull(fc);
    }

    /**
     * Construct bonded to a filesystem.
     * @param fs file system.
     */
    public GlobBuilder(final FileSystem fs) {
      this.fs = checkNotNull(fs);
      this.fc = null;
    }

    /**
     * Set the path pattern.
     * @param pattern pattern to use.
     * @return the builder
     */
    public GlobBuilder withPathPattern(Path pattern) {
      pathPattern = pattern;
      return this;
    }

    /**
     * Set the path filter.
     * @param pathFilter filter
     * @return the builder
     */
    public GlobBuilder withPathFiltern(PathFilter pathFilter) {
      filter = pathFilter;
      return this;
    }

    /**
     * Set the symlink resolution policy.
     * @param resolve resolution flag.
     * @return the builder
     */
    public GlobBuilder withResolveSymlinks(boolean resolve) {
      resolveSymlinks = resolve;
      return this;
    }

    /**
     * Build the Globber.
     * @return a new instance.
     */
    public Globber build() {
      return fs != null
          ? new Globber(fs, pathPattern, filter, resolveSymlinks)
          : new Globber(fc, pathPattern, filter, resolveSymlinks);
    }
  }
}
